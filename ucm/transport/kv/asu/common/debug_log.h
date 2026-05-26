#pragma once

#include <chrono>
#include <cstdio>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>

#ifndef ASU_DEBUG_LOG_ENABLED
#define ASU_DEBUG_LOG_ENABLED 1
#endif

namespace UC::ASU {

inline std::string debug_timestamp()
{
    auto now = std::chrono::system_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                  now.time_since_epoch()) % 1000;
    auto t = std::chrono::system_clock::to_time_t(now);
    struct tm tm_buf;
#ifdef _WIN32
    localtime_s(&tm_buf, &t);
#else
    localtime_r(&t, &tm_buf);
#endif
    char buf[32];
    std::snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d.%03d",
                  tm_buf.tm_year + 1900, tm_buf.tm_mon + 1, tm_buf.tm_mday,
                  tm_buf.tm_hour, tm_buf.tm_min, tm_buf.tm_sec,
                  static_cast<int>(ms.count()));
    return buf;
}

inline std::string debug_tid()
{
    std::ostringstream oss;
    oss << std::this_thread::get_id();
    return oss.str();
}

inline void debug_log(const std::string& tag, const std::string& msg)
{
#if ASU_DEBUG_LOG_ENABLED
    std::cout << "[" << debug_timestamp() << "][tid:" << debug_tid()
              << "][DEBUG][" << tag << "] " << msg << "\n";
#else
    (void)tag;
    (void)msg;
#endif
}

}  // namespace UC::ASU