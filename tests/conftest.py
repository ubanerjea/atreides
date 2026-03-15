
### keep output

def pytest_addoption(parser):
    parser.addoption(
        "--keep-output",
        action="store_true",
        default=False,
        help="Preserve generated output files after test_explain runs.",
    )

# conftest.py is a special pytest file that's automatically loaded before any tests run. It's the right place to register custom CLI options because pytest discovers and loads it without you having to import it anywhere.

# Here's the chain when you run pytest tests/test_explain.py -v --keep-output:

# 1. pytest loads conftest.py first


# def pytest_addoption(parser):
#     parser.addoption("--keep-output", action="store_true", default=False, ...)
# pytest_addoption is a pytest hook — pytest calls it automatically during startup to let you add custom flags to its argument parser. This is what makes --keep-output a recognised flag rather than an "unrecognised argument" error.

# 2. pytest parses the CLI args
# It sees --keep-output in the command, matches it to the registered option, and stores True in the session config. action="store_true" means no value is expected after the flag — its presence alone sets it to True, absence leaves it at default=False.

# 3. The test runs and reads the value via request


# def test_explain_and_save_all_modes(request):
#     keep_output = request.config.getoption("--keep-output")
# request is a built-in pytest fixture that gives access to the current test's context. request.config is the session-wide config object — the same one that stored the parsed CLI value in step 2. getoption("--keep-output") retrieves it, giving True or False.

# 4. Cleanup is conditionally skipped


# if not keep_output:
#     for path in generated:
#         path.unlink()
# True → cleanup skipped, files stay. False (default) → files are deleted.

###