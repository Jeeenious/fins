/*******************************************************************************
 * Copyright (c) 2025.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 ******************************************************************************/

// hello_world.hpp

#include <atomic>
#include <chrono>
#include <fins/node.hpp>
#include <fins/utils/time.hpp>
#include <thread>

class HelloWorldSource : public fins::Node {
public:
  HelloWorldSource() = default;

  void define() override {
    set_name("HelloWorldSource");
    set_description("Generates hello world messages");
    set_category("HelloWorld");

    register_output<0, std::string>("str");
  }

  void initialize() override {
    logger->info("Initialized.");
    running_ = false;
  }
  const std::string msg = "Hello, World!";

  void run() override {
    if (running_)
      return;
    running_ = true;

    worker_ = std::thread([this]() {
      logger->info("Worker thread started.");
      while (running_) {
        this->send<0>(msg, fins::now());
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
      }
    });
  }

  void pause() override {
    running_ = false;
    if (worker_.joinable()) {
      worker_.join();
    }
    logger->info("Paused.");
  }

  void reset() override { logger->info("Reset."); }

private:
  std::thread worker_;
  std::atomic<bool> running_{false};
};

EXPORT_NODE(HelloWorldSource)

class HelloWorldPrinter : public fins::Node {
public:
  HelloWorldPrinter() = default;

  void define() override {
    set_name("HelloWorldPrinter");
    set_description("Prints hello world messages to console");
    set_category("HelloWorld");

    register_input<0, std::string>("str", &HelloWorldPrinter::receive_msg);
  }

  void initialize() override { logger->info("Initialized."); }

  void run() override { logger->info("Running."); }

  void pause() override {}

  void reset() override { logger->info("Reset."); }

  void receive_msg(const fins::Msg<std::string> &msg) {
    if (!msg)
      return;

    static size_t count = 0;
    static uint64_t total_latency = 0;

    total_latency += fins::latency_us(msg.acq_time);

    if (++count >= 100) {
      logger->info("Received 100 messages. Average Latency: {} us",
                   (total_latency / 100.0));
      count = 0;
      total_latency = 0;
    }
  }
};
EXPORT_NODE(HelloWorldPrinter)