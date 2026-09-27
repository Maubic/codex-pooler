defmodule CodexPooler.RequestCompressionFixtures do
  @moduledoc false

  # Verbatim stderr of Apple clang 21.0.0 (`clang -fsyntax-only -ferror-limit=0
  # -fno-color-diagnostics src/sample.c`) for a synthetic file whose 24 lines
  # each read an undeclared identifier.
  @clang_undeclared_identifier_diagnostics ~S"""
  src/sample.c:1:35: error: use of undeclared identifier 'missing_value_1'
      1 | int sample_value_1(void) { return missing_value_1; }
        |                                   ^~~~~~~~~~~~~~~
  src/sample.c:2:35: error: use of undeclared identifier 'missing_value_2'
      2 | int sample_value_2(void) { return missing_value_2; }
        |                                   ^~~~~~~~~~~~~~~
  src/sample.c:3:35: error: use of undeclared identifier 'missing_value_3'
      3 | int sample_value_3(void) { return missing_value_3; }
        |                                   ^~~~~~~~~~~~~~~
  src/sample.c:4:35: error: use of undeclared identifier 'missing_value_4'
      4 | int sample_value_4(void) { return missing_value_4; }
        |                                   ^~~~~~~~~~~~~~~
  src/sample.c:5:35: error: use of undeclared identifier 'missing_value_5'
      5 | int sample_value_5(void) { return missing_value_5; }
        |                                   ^~~~~~~~~~~~~~~
  src/sample.c:6:35: error: use of undeclared identifier 'missing_value_6'
      6 | int sample_value_6(void) { return missing_value_6; }
        |                                   ^~~~~~~~~~~~~~~
  src/sample.c:7:35: error: use of undeclared identifier 'missing_value_7'
      7 | int sample_value_7(void) { return missing_value_7; }
        |                                   ^~~~~~~~~~~~~~~
  src/sample.c:8:35: error: use of undeclared identifier 'missing_value_8'
      8 | int sample_value_8(void) { return missing_value_8; }
        |                                   ^~~~~~~~~~~~~~~
  src/sample.c:9:35: error: use of undeclared identifier 'missing_value_9'
      9 | int sample_value_9(void) { return missing_value_9; }
        |                                   ^~~~~~~~~~~~~~~
  src/sample.c:10:36: error: use of undeclared identifier 'missing_value_10'
     10 | int sample_value_10(void) { return missing_value_10; }
        |                                    ^~~~~~~~~~~~~~~~
  src/sample.c:11:36: error: use of undeclared identifier 'missing_value_11'
     11 | int sample_value_11(void) { return missing_value_11; }
        |                                    ^~~~~~~~~~~~~~~~
  src/sample.c:12:36: error: use of undeclared identifier 'missing_value_12'
     12 | int sample_value_12(void) { return missing_value_12; }
        |                                    ^~~~~~~~~~~~~~~~
  src/sample.c:13:36: error: use of undeclared identifier 'missing_value_13'
     13 | int sample_value_13(void) { return missing_value_13; }
        |                                    ^~~~~~~~~~~~~~~~
  src/sample.c:14:36: error: use of undeclared identifier 'missing_value_14'
     14 | int sample_value_14(void) { return missing_value_14; }
        |                                    ^~~~~~~~~~~~~~~~
  src/sample.c:15:36: error: use of undeclared identifier 'missing_value_15'
     15 | int sample_value_15(void) { return missing_value_15; }
        |                                    ^~~~~~~~~~~~~~~~
  src/sample.c:16:36: error: use of undeclared identifier 'missing_value_16'
     16 | int sample_value_16(void) { return missing_value_16; }
        |                                    ^~~~~~~~~~~~~~~~
  src/sample.c:17:36: error: use of undeclared identifier 'missing_value_17'
     17 | int sample_value_17(void) { return missing_value_17; }
        |                                    ^~~~~~~~~~~~~~~~
  src/sample.c:18:36: error: use of undeclared identifier 'missing_value_18'
     18 | int sample_value_18(void) { return missing_value_18; }
        |                                    ^~~~~~~~~~~~~~~~
  src/sample.c:19:36: error: use of undeclared identifier 'missing_value_19'
     19 | int sample_value_19(void) { return missing_value_19; }
        |                                    ^~~~~~~~~~~~~~~~
  src/sample.c:20:36: error: use of undeclared identifier 'missing_value_20'
     20 | int sample_value_20(void) { return missing_value_20; }
        |                                    ^~~~~~~~~~~~~~~~
  src/sample.c:21:36: error: use of undeclared identifier 'missing_value_21'
     21 | int sample_value_21(void) { return missing_value_21; }
        |                                    ^~~~~~~~~~~~~~~~
  src/sample.c:22:36: error: use of undeclared identifier 'missing_value_22'
     22 | int sample_value_22(void) { return missing_value_22; }
        |                                    ^~~~~~~~~~~~~~~~
  src/sample.c:23:36: error: use of undeclared identifier 'missing_value_23'
     23 | int sample_value_23(void) { return missing_value_23; }
        |                                    ^~~~~~~~~~~~~~~~
  src/sample.c:24:36: error: use of undeclared identifier 'missing_value_24'
     24 | int sample_value_24(void) { return missing_value_24; }
        |                                    ^~~~~~~~~~~~~~~~
  24 errors generated.
  """

  @spec clang_undeclared_identifier_diagnostics() :: String.t()
  def clang_undeclared_identifier_diagnostics, do: @clang_undeclared_identifier_diagnostics
end
