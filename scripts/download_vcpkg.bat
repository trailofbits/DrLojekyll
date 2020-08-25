@echo off

set cur_dir=%~dp0
set repo_root="%cur_dir%.."
call :NORMALIZEPATH %repo_root%
SET repo_root=%RETVAL%

for /f "tokens=2" %%a in ("%file%") do (
    Echo vcpkg contains spaces. Proceed at your own risk.
)

:: default arguments
set /p vcpkg_commit=<"%repo_root%\vcpkg_commit.txt"
set vcpkg_url=https://github.com/microsoft/vcpkg.git
set drlog_vcpkg_dir="%repo_root%\vcpkg"

:: Parse command line options by name
:: https://stackoverflow.com/a/32078177
:initial
if "%1"=="" goto done
if "%1"=="-h" goto usage
if "%1"=="--help" goto usage
set aux=%1
if "%aux:~0,1%"=="-" (
   set nome=%aux:~1,250%
) else (
   set "%nome%=%1"
   set nome=
)
shift
goto initial
:done

Echo Setting up vcpkg from %vcpkg_url%@%vcpkg_commit% in %drlog_vcpkg_dir%
Echo.

IF NOT EXIST "%drlog_vcpkg_dir%" (
   git clone -n "%vcpkg_url%" "%drlog_vcpkg_dir%" || exit /b
)
cd "%drlog_vcpkg_dir%" || exit /b

:: Read the current git commit
FOR /F %%i IN ('git rev-parse HEAD') DO set current_git_commit=%%i
:: Count number of files
set file_count=0 & for %%f in (*) do @(set /a file_count+=1 > nul)
:: Check if all but the last character matches our chosen commit
IF NOT "%current_git_commit:~0,-1%"=="%vcpkg_commit%" goto true_label
IF %file_count% LEQ 5 goto true_label
:: None of the above hold...
goto end

:true_label
git fetch || exit /b
git checkout "%vcpkg_commit%" || exit /b
IF NOT DEFINED CI (
   CALL bootstrap-vcpkg.bat || exit /b
   cd "%repo_root%"
   "%drlog_vcpkg_dir%\vcpkg.exe" install "@vcpkg.txt" || exit /b
   "%drlog_vcpkg_dir%\vcpkg.exe" upgrade --no-dry-run || exit /b
)

:end

cd "%repo_root%" || exit /b

Echo.
Echo Done setting up vcpkg
Echo Please run the following to install DrLojekyll dependencies
Echo.
Echo To compile DrLojekyll with vcpkg dependencies, please add the following to your
Echo CMake invocation:
Echo.
Echo   -DCMAKE_TOOLCHAIN_FILE=%drlog_vcpkg_dir%\scripts\buildsystems\vcpkg.cmake
Echo.
Echo See https://github.com/microsoft/vcpkg for more details regarding vcpkg


:usage
Echo.
Echo Run the program with none or some of the following options
Echo   -vcpkg_commit commit
Echo   -vcpkg_url url
Echo   -drlog_vcpkg_dir directory

:: ========== FUNCTIONS ==========
EXIT /B

:NORMALIZEPATH
  SET RETVAL=%~f1
  EXIT /B
