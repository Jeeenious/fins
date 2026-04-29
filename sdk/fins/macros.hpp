#pragma once

#if defined(_WIN32) || defined(__CYGWIN__)
    #ifdef FINS_EXPORTS
        #define FINS_API __declspec(dllexport)
    #else
        #define FINS_API __declspec(dllimport)
    #endif
#else
    #if __GNUC__ >= 4
        #define FINS_API __attribute__ ((visibility ("default")))
    #else
        #define FINS_API
    #endif
#endif