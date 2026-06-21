cmake_minimum_required(VERSION 3.24)
project(amm_simulator VERSION 1.0.0 LANGUAGES C CXX)

option(AMM_BUILD_TESTS "Build the test suite" ON)
option(AMM_ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(AMM_ENABLE_UBSAN "Enable UndefinedBehaviorSanitizer" OFF)
option(AMM_ENABLE_TSAN "Enable ThreadSanitizer" OFF)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

include(FetchContent)
find_package(Threads REQUIRED)
FetchContent_Declare(nlohmann_json
  URL https://github.com/nlohmann/json/releases/download/v3.11.3/json.tar.xz
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE)
FetchContent_MakeAvailable(nlohmann_json)

# SQLite's versioned amalgamation archive is immutable. We compile the C source
# directly so every platform uses the same SQLite release.
FetchContent_Declare(sqlite_amalgamation
  URL https://www.sqlite.org/2024/sqlite-amalgamation-3460100.zip
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE)
FetchContent_MakeAvailable(sqlite_amalgamation)
add_library(sqlite3 STATIC "${sqlite_amalgamation_SOURCE_DIR}/sqlite3.c")
target_include_directories(sqlite3 PUBLIC "${sqlite_amalgamation_SOURCE_DIR}")
if(UNIX)
  target_link_libraries(sqlite3 PUBLIC ${CMAKE_DL_LIBS} Threads::Threads)
endif()

add_library(amm_core
  src/config.cpp
  src/engine.cpp
  src/ledger.cpp)
target_include_directories(amm_core PUBLIC include)
target_link_libraries(amm_core PUBLIC nlohmann_json::nlohmann_json sqlite3 Threads::Threads)

if(MSVC)
  target_compile_options(amm_core PRIVATE /W4 /permissive-)
else()
  target_compile_options(amm_core PRIVATE -Wall -Wextra -Wpedantic -Wconversion -Wshadow)
endif()

set(_sanitizers "")
if(AMM_ENABLE_ASAN)
  list(APPEND _sanitizers address)
endif()
if(AMM_ENABLE_UBSAN)
  list(APPEND _sanitizers undefined)
endif()
if(AMM_ENABLE_TSAN)
  if(AMM_ENABLE_ASAN)
    message(FATAL_ERROR "ThreadSanitizer and AddressSanitizer cannot be enabled together")
  endif()
  list(APPEND _sanitizers thread)
endif()
if(_sanitizers AND NOT MSVC)
  list(JOIN _sanitizers "," _sanitizer_flags)
  target_compile_options(amm_core PUBLIC "-fsanitize=${_sanitizer_flags}" -fno-omit-frame-pointer)
  target_link_options(amm_core PUBLIC "-fsanitize=${_sanitizer_flags}")
endif()

add_executable(amm-sim src/main.cpp)
target_link_libraries(amm-sim PRIVATE amm_core)

if(AMM_BUILD_TESTS)
  enable_testing()
  FetchContent_Declare(Catch2
    GIT_REPOSITORY https://github.com/catchorg/Catch2.git
    GIT_TAG v3.7.1
    GIT_SHALLOW TRUE)
  FetchContent_MakeAvailable(Catch2)
  add_executable(amm-tests tests/test_core.cpp)
  target_link_libraries(amm-tests PRIVATE amm_core Catch2::Catch2WithMain)
  target_compile_definitions(amm-tests PRIVATE AMM_SIM_PATH="$<TARGET_FILE:amm-sim>")
  add_dependencies(amm-tests amm-sim)
  include(Catch)
  catch_discover_tests(amm-tests)
endif()
