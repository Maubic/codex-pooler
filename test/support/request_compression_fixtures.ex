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

  # Verbatim output of Apple diff (`diff -ru -U 10 a b`) for synthetic trees:
  # three changed text files (one deleting a line that starts with `--`), a
  # binary file, and a file only in `a`.
  @recursive_unified_diff ~S"""
  Binary files a/logo.bin and b/logo.bin differ
  diff -ru -U 10 a/notes.txt b/notes.txt
  --- a/notes.txt	2026-09-27 09:00:00
  +++ b/notes.txt	2026-09-27 10:00:00
  @@ -20,21 +20,21 @@
   alpha synthetic line 20
   alpha synthetic line 21
   alpha synthetic line 22
   alpha synthetic line 23
   alpha synthetic line 24
   alpha synthetic line 25
   alpha synthetic line 26
   alpha synthetic line 27
   alpha synthetic line 28
   alpha synthetic line 29
  -alpha synthetic line 30
  +alpha synthetic line 30 changed
   alpha synthetic line 31
   alpha synthetic line 32
   alpha synthetic line 33
   alpha synthetic line 34
   alpha synthetic line 35
   alpha synthetic line 36
   alpha synthetic line 37
   alpha synthetic line 38
   alpha synthetic line 39
   alpha synthetic line 40
  Only in a: only_old.txt
  diff -ru -U 10 a/query.sql b/query.sql
  --- a/query.sql	2026-09-27 09:00:00
  +++ b/query.sql	2026-09-27 10:00:00
  @@ -1,21 +1,20 @@
   select 1;
   -- synthetic comment 1
   -- synthetic comment 2
   -- synthetic comment 3
   -- synthetic comment 4
   -- synthetic comment 5
   -- synthetic comment 6
   -- synthetic comment 7
   -- synthetic comment 8
   -- synthetic comment 9
  --- synthetic comment 10
   -- synthetic comment 11
   -- synthetic comment 12
   -- synthetic comment 13
   -- synthetic comment 14
   -- synthetic comment 15
   -- synthetic comment 16
   -- synthetic comment 17
   -- synthetic comment 18
   -- synthetic comment 19
   -- synthetic comment 20
  diff -ru -U 10 a/report.txt b/report.txt
  --- a/report.txt	2026-09-27 09:00:00
  +++ b/report.txt	2026-09-27 10:00:00
  @@ -1,20 +1,20 @@
   beta synthetic line 1
   beta synthetic line 2
   beta synthetic line 3
   beta synthetic line 4
   beta synthetic line 5
   beta synthetic line 6
   beta synthetic line 7
   beta synthetic line 8
   beta synthetic line 9
  -beta synthetic line 10
  +beta synthetic line 10 revised
   beta synthetic line 11
   beta synthetic line 12
   beta synthetic line 13
   beta synthetic line 14
   beta synthetic line 15
   beta synthetic line 16
   beta synthetic line 17
   beta synthetic line 18
   beta synthetic line 19
   beta synthetic line 20
  @@ -40,21 +40,21 @@
   beta synthetic line 40
   beta synthetic line 41
   beta synthetic line 42
   beta synthetic line 43
   beta synthetic line 44
   beta synthetic line 45
   beta synthetic line 46
   beta synthetic line 47
   beta synthetic line 48
   beta synthetic line 49
  -beta synthetic line 50
  +beta synthetic line 50 revised
   beta synthetic line 51
   beta synthetic line 52
   beta synthetic line 53
   beta synthetic line 54
   beta synthetic line 55
   beta synthetic line 56
   beta synthetic line 57
   beta synthetic line 58
   beta synthetic line 59
   beta synthetic line 60
  """

  @spec recursive_unified_diff() :: String.t()
  def recursive_unified_diff, do: @recursive_unified_diff

  # Verbatim output of Apple sort 2.3 (`sort --files0-from=paths.nul`, the list
  # naming a synthetic file of 24 grep-shaped diagnostics); `sort` on the file
  # itself prints the same bytes.
  @sorted_file_contents ~S"""
  lib/sample_0.go:12:2: should omit type in synthetic declaration 12
  lib/sample_0.go:15:2: should omit type in synthetic declaration 15
  lib/sample_0.go:18:2: should omit type in synthetic declaration 18
  lib/sample_0.go:21:2: should omit type in synthetic declaration 21
  lib/sample_0.go:24:2: should omit type in synthetic declaration 24
  lib/sample_0.go:3:2: should omit type in synthetic declaration 3
  lib/sample_0.go:6:2: should omit type in synthetic declaration 6
  lib/sample_0.go:9:2: should omit type in synthetic declaration 9
  lib/sample_1.go:10:2: should omit type in synthetic declaration 10
  lib/sample_1.go:13:2: should omit type in synthetic declaration 13
  lib/sample_1.go:16:2: should omit type in synthetic declaration 16
  lib/sample_1.go:19:2: should omit type in synthetic declaration 19
  lib/sample_1.go:1:2: should omit type in synthetic declaration 1
  lib/sample_1.go:22:2: should omit type in synthetic declaration 22
  lib/sample_1.go:4:2: should omit type in synthetic declaration 4
  lib/sample_1.go:7:2: should omit type in synthetic declaration 7
  lib/sample_2.go:11:2: should omit type in synthetic declaration 11
  lib/sample_2.go:14:2: should omit type in synthetic declaration 14
  lib/sample_2.go:17:2: should omit type in synthetic declaration 17
  lib/sample_2.go:20:2: should omit type in synthetic declaration 20
  lib/sample_2.go:23:2: should omit type in synthetic declaration 23
  lib/sample_2.go:2:2: should omit type in synthetic declaration 2
  lib/sample_2.go:5:2: should omit type in synthetic declaration 5
  lib/sample_2.go:8:2: should omit type in synthetic declaration 8
  """

  @spec sorted_file_contents() :: String.t()
  def sorted_file_contents, do: @sorted_file_contents
end
