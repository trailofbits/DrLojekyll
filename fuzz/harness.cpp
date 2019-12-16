#include <drlojekyll/Display/DisplayConfiguration.h>
#include <drlojekyll/Display/DisplayManager.h>
#include <drlojekyll/Parse/ErrorLog.h>
#include <drlojekyll/Parse/Parser.h>
#include <drlojekyll/Parse/Format.h>

#include <deepstate/DeepState.hpp>

using namespace deepstate;

#ifdef LIBFUZZER_FALLBACK

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *Data, size_t Size) {
  std::string_view data(reinterpret_cast<const char *>(Data), Size);
  hyde::DisplayManager display_manager;
  hyde::ErrorLog error_log;
  hyde::Parser parser(display_manager, error_log);
  const std::string target_name = "harness_module";
  hyde::DisplayConfiguration config = {
          target_name, //  `name`.
          2,  //  `num_spaces_in_tab`.
          true, //  `use_tab_stops`.
  };
  auto module = parser.ParseBuffer(data, config);

  if (error_log.IsEmpty()) {
    config.name = "verified_harness_module";
    hyde::DisplayManager v_display_manager;
    hyde::ErrorLog v_error_log;
    hyde::Parser v_parser(v_display_manager, v_error_log);
    std::stringstream format_stream;
    std::stringstream verify_stream;
    hyde::OutputStream os(display_manager, format_stream);
    os << module;
    const auto format_stream_string = format_stream.str();
    std::cerr << format_stream_string;
    auto module2 = v_parser.ParseBuffer(format_stream_string, config);

    hyde::OutputStream os2(v_display_manager, verify_stream);
    os2 << module2;

    v_error_log.Render(std::cerr);
    assert(v_error_log.IsEmpty());
    assert(verify_stream.str() == format_stream_string);
  }

  return 0;
}

#else

class DrLojekyll: public Test {
  public:

    hyde::DisplayManager dm;
    hyde::ErrorLog el;

    hyde::DisplayConfiguration config = {
      "deepstate_fuzz", 2, true
    };

    DrLojekyll() {
      hyde::Parser parser(dm, el);
    }

    void SetUp(void) {
      LOG(TRACE) << "Initializing parser";
    }

    void TearDown(void) {
      LOG(TRACE) << "Cleaning up";
    }
};


TEST_F(DrLojekyll, ParseAndVerify) {
  char *input = DeepState_CStr(8192);
  auto module = parser.ParseBuffer(input, config);
}


TEST_F(DrLojekyll, ParseAndVerifyFile) {
    const char * path = DeepState_InputPath(NULL);
    auto module = parser.ParsePath(path, config);
}


TEST_F(DrLojekyll, ParseAndVerifyStream) {
  char *input = DeepState_CStr(8192);
  auto module = parser.ParseBuffer(input, config);

  std::stringstream format_stream;
  std::stringstream verify_stream;

  hyde::OutputStream os(dm, format_stream);
  hyde::FormatModule(os, module);

  hyde::OutputStream os2(dm, verify_stream);
  hyde::FormatModule(os2, module2);

  el.Render(std::cerr);
  ASSERT(el.IsEmpty());
  ASSERT_EQ(verify_stream.str(), format_stream_str);
}

#endif
