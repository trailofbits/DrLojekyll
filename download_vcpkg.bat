@echo off

set cur_dir=%~dp0
for /f "tokens=2" %%a in ("%file%") do (
    Echo vcpkg contains spaces. Proceed at your own risk.
)

:: default arguments
set /p vcpkg_commit=<"%cur_dir%\vcpkg_commit.txt"
set vcpkg_url="https://github.com/microsoft/vcpkg.git"
set vcpkg_directory="%cur_dir%\vcpkg"

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

Echo Setting up vcpkg from %vcpkg_url%@%vcpkg_commit% in %vcpkg_directory%
Echo.

IF NOT EXIST "%vcpkg_directory%" (
    git clone -n "%vcpkg_url%" "%vcpkg_directory%" > NUL 2>&1 || exit /b
)
cd "%vcpkg_directory%" || exit /b
:: Read the current git commit
FOR /F %%i IN ('git rev-parse HEAD') DO set current_git_commit=%%i
:: Check if all but the last character matches our chosen commit
IF NOT "%current_git_commit:~0,-1%"=="%vcpkg_commit%" (
    git checkout "%vcpkg_commit%" || exit /b
)
cd "%cur_dir%" > NUL 2>&1 || exit /b

Echo.
Echo Done setting up vcpkg
Echo Please run the following to install DrLojekyll dependencies
Echo.
Echo   $ %vcpkg_directory%\bootstrap-vcpkg.bat
Echo   $ %vcpkg_directory%\vcpkg install @vcpkg.txt
Echo.
Echo To compile DrLojekyll with vcpkg dependencies, please add the following to your
Echo CMake invocation:
Echo.
Echo   -DCMAKE_TOOLCHAIN_FILE=%vcpkg_directory%\scripts\buildsystems\vcpkg.cmake
Echo.
Echo See https://github.com/microsoft/vcpkg for more details regarding vcpkg


:usage
Echo.
Echo Run the program with none or some of the following options
Echo   -vcpkg_commit commit
Echo   -vcpkg_url url
Echo   -vcpkg_directory directory
