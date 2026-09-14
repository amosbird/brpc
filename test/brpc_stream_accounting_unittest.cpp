// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <atomic>
#include <chrono>
#include <memory>
#include <vector>
#include <gtest/gtest.h>
#include "brpc/channel.h"
#include "brpc/controller.h"
#include "brpc/server.h"
#include "brpc/stream_impl.h"
#include "echo.pb.h"

namespace brpc {
DECLARE_int64(socket_max_streams_unconsumed_bytes);
}

namespace {

template <typename Predicate>
bool WaitForAccounting(Predicate predicate) {
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::seconds(5);
    while (!predicate()) {
        if (std::chrono::steady_clock::now() >= deadline) {
            return false;
        }
        bthread_usleep(1000);
    }
    return true;
}

struct AccountingHandler : brpc::StreamInputHandler {
    AccountingHandler() : blocked(true), entered(false), closed(false) {}

    int on_received_messages(brpc::StreamId, butil::IOBuf* const[],
                             size_t) override {
        entered.store(true);
        while (blocked.load()) {
            bthread_usleep(100);
        }
        return 0;
    }
    void on_idle_timeout(brpc::StreamId) override {}
    void on_closed(brpc::StreamId) override { closed.store(true); }

    std::atomic<bool> blocked;
    std::atomic<bool> entered;
    std::atomic<bool> closed;
};

struct AccountingStream {
    AccountingStream() : id(brpc::INVALID_STREAM_ID),
                         peer_id(brpc::INVALID_STREAM_ID) {}
    brpc::StreamId id;
    brpc::StreamId peer_id;
    AccountingHandler sender;
    AccountingHandler receiver;
    brpc::SocketUniquePtr host;
};

class AccountingService : public test::EchoService {
public:
    AccountingService() : next(NULL) {}
    void Echo(google::protobuf::RpcController* controller,
              const test::EchoRequest* request, test::EchoResponse* response,
              google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        AccountingStream* stream = next.load();
        brpc::StreamOptions options;
        options.handler = &stream->receiver;
        brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
        if (brpc::StreamAccept(&stream->peer_id, *cntl, &options) != 0) {
            cntl->SetFailed("Cannot accept accounting test stream");
            return;
        }
        response->set_message(request->message());
    }
    std::atomic<AccountingStream*> next;
};

class StreamAccountingTest : public testing::Test {
protected:
    void SetUp() override {
        _saved_limit = brpc::FLAGS_socket_max_streams_unconsumed_bytes;
        brpc::FLAGS_socket_max_streams_unconsumed_bytes = 128LL << 20;
        ASSERT_EQ(0, _server.AddService(&_service,
                                       brpc::SERVER_DOESNT_OWN_SERVICE));
        ASSERT_EQ(0, _server.Start("127.0.0.1:0", NULL));
        brpc::ChannelOptions options;
        options.connection_type = "single";
        options.timeout_ms = 5000;
        options.max_retry = 0;
        ASSERT_EQ(0, _channel.Init(_server.listen_address(), &options));
    }

    void TearDown() override {
        // Unblock callbacks even when an assertion fails, before joining server.
        for (const auto& stream : _streams) {
            stream->sender.blocked.store(false);
            stream->receiver.blocked.store(false);
            if (stream->id != brpc::INVALID_STREAM_ID) {
                brpc::StreamClose(stream->id);
            }
            if (stream->peer_id != brpc::INVALID_STREAM_ID) {
                brpc::StreamClose(stream->peer_id);
            }
        }
        _server.Stop(0);
        _server.Join();
        for (const auto& stream : _streams) {
            EXPECT_TRUE(WaitForAccounting([&] {
                return (stream->id == brpc::INVALID_STREAM_ID ||
                        stream->sender.closed.load()) &&
                       (stream->peer_id == brpc::INVALID_STREAM_ID ||
                        stream->receiver.closed.load());
            }));
        }
        brpc::FLAGS_socket_max_streams_unconsumed_bytes = _saved_limit;
    }

    AccountingStream* Open(int64_t max_buf_size = 16 << 20,
                           int64_t min_buf_size = 1 << 20) {
        _streams.emplace_back(new AccountingStream);
        AccountingStream* stream = _streams.back().get();
        _service.next.store(stream);
        brpc::Controller cntl;
        brpc::StreamOptions options;
        options.handler = &stream->sender;
        options.max_buf_size = max_buf_size;
        options.min_buf_size = min_buf_size;
        if (brpc::StreamCreate(&stream->id, cntl, &options) != 0) {
            ADD_FAILURE() << "StreamCreate failed";
            return NULL;
        }
        test::EchoRequest request;
        test::EchoResponse response;
        request.set_message("accounting");
        test::EchoService_Stub stub(&_channel);
        stub.Echo(&cntl, &request, &response, NULL);
        if (cntl.Failed()) {
            ADD_FAILURE() << cntl.ErrorText();
            return NULL;
        }
        brpc::SocketUniquePtr ptr;
        if (brpc::Socket::Address(stream->id, &ptr) != 0) {
            ADD_FAILURE() << "Cannot address connected stream";
            return NULL;
        }
        static_cast<brpc::Stream*>(ptr->conn())->_host_socket->ReAddress(
            &stream->host);
        return stream;
    }

    static int64_t Total(const AccountingStream* stream) {
        return stream->host->_total_streams_unconsumed_size.load(
            butil::memory_order_relaxed);
    }

    static void WriteBlocked(AccountingStream* stream) {
        butil::IOBuf data;
        data.append(std::string(3 << 20, 'x'));
        ASSERT_EQ(0, brpc::StreamWrite(stream->id, data));
        ASSERT_TRUE(WaitForAccounting([&] {
            return stream->receiver.entered.load();
        }));
    }

    static bool CloseSender(AccountingStream* stream) {
        return brpc::StreamClose(stream->id) == 0 &&
               WaitForAccounting([&] { return stream->sender.closed.load(); });
    }

    int64_t _saved_limit;
    std::vector<std::unique_ptr<AccountingStream> > _streams;
    AccountingService _service;
    brpc::Server _server;
    brpc::Channel _channel;
};

// Backported from Apache brpc #3422: closing before the receiver returns its
// feedback must retire this stream's outstanding contribution to the socket.
TEST_F(StreamAccountingTest, unconsumed_bytes_reclaimed_on_stream_close) {
    AccountingStream* stream = Open();
    ASSERT_NE(nullptr, stream);
    ASSERT_NO_FATAL_FAILURE(WriteBlocked(stream));
    ASSERT_EQ(3 << 20, Total(stream));
    ASSERT_TRUE(CloseSender(stream));
    EXPECT_EQ(0, Total(stream));
}

TEST_F(StreamAccountingTest, closing_stream_preserves_active_stream_credit) {
    AccountingStream* active = Open();
    AccountingStream* retired = Open();
    ASSERT_NE(nullptr, active);
    ASSERT_NE(nullptr, retired);
    ASSERT_EQ(active->host->id(), retired->host->id());
    ASSERT_NO_FATAL_FAILURE(WriteBlocked(active));
    ASSERT_NO_FATAL_FAILURE(WriteBlocked(retired));
    ASSERT_EQ(6 << 20, Total(active));
    ASSERT_TRUE(CloseSender(retired));
    ASSERT_EQ(3 << 20, Total(active));
    active->receiver.blocked.store(false);
    ASSERT_TRUE(WaitForAccounting([&] { return Total(active) == 0; }));
    ASSERT_TRUE(CloseSender(active));
    EXPECT_EQ(0, Total(active));
}

TEST_F(StreamAccountingTest, cleanup_waits_for_last_reference) {
    AccountingStream* stream = Open();
    ASSERT_NE(nullptr, stream);
    ASSERT_NO_FATAL_FAILURE(WriteBlocked(stream));
    brpc::SocketUniquePtr held;
    ASSERT_EQ(0, brpc::Socket::Address(stream->id, &held));
    ASSERT_EQ(0, brpc::StreamClose(stream->id));
    EXPECT_FALSE(stream->sender.closed.load());
    EXPECT_EQ(3 << 20, Total(stream));
    held.reset();
    ASSERT_TRUE(WaitForAccounting([&] { return stream->sender.closed.load(); }));
    EXPECT_EQ(0, Total(stream));
    brpc::StreamClose(stream->id);
    stream->receiver.blocked.store(false);
    ASSERT_TRUE(WaitForAccounting([&] { return stream->receiver.closed.load(); }));
    EXPECT_EQ(0, Total(stream));
}

TEST_F(StreamAccountingTest, unlimited_stream_credit_is_reclaimed) {
    AccountingStream* stream = Open(0, 0);
    ASSERT_NE(nullptr, stream);
    ASSERT_NO_FATAL_FAILURE(WriteBlocked(stream));
    ASSERT_EQ(3 << 20, Total(stream));
    ASSERT_TRUE(CloseSender(stream));
    EXPECT_EQ(0, Total(stream));
}

TEST_F(StreamAccountingTest, disabling_limit_does_not_abandon_existing_credit) {
    AccountingStream* stream = Open();
    ASSERT_NE(nullptr, stream);
    ASSERT_NO_FATAL_FAILURE(WriteBlocked(stream));
    brpc::FLAGS_socket_max_streams_unconsumed_bytes = 0;
    ASSERT_TRUE(CloseSender(stream));
    EXPECT_EQ(0, Total(stream));
}

TEST_F(StreamAccountingTest, repeated_stream_close_does_not_accumulate_credit) {
    brpc::SocketId host_id = 0;
    for (int i = 0; i < 44; ++i) {
        AccountingStream* stream = Open();
        ASSERT_NE(nullptr, stream);
        if (i == 0) {
            host_id = stream->host->id();
        }
        ASSERT_EQ(host_id, stream->host->id());
        ASSERT_NO_FATAL_FAILURE(WriteBlocked(stream));
        ASSERT_TRUE(CloseSender(stream));
        ASSERT_EQ(0, Total(stream)) << "closed stream=" << i;
        stream->receiver.blocked.store(false);
        ASSERT_TRUE(WaitForAccounting([&] {
            return stream->receiver.closed.load();
        }));
    }
}

} // namespace
