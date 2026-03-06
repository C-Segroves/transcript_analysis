"""
Master test script that runs all test suites.
"""

import sys
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def run_test_suite(module_name, test_function):
    """Run a test suite and report results."""
    logger.info("\n" + "=" * 80)
    logger.info(f"Running {module_name}")
    logger.info("=" * 80)
    
    try:
        test_function()
        logger.info(f"✓ {module_name} completed successfully")
        return True
    except Exception as e:
        logger.error(f"✗ {module_name} failed with error: {e}", exc_info=True)
        return False


def main():
    """Run all test suites."""
    logger.info("\n" + "=" * 80)
    logger.info("DATABASE MIGRATION TEST SUITE")
    logger.info("=" * 80)
    logger.info("\nThis test suite verifies that all functions work correctly")
    logger.info("with the database schema. Most tests use transactions")
    logger.info("with rollback to avoid permanent changes.\n")
    
    results = {}
    
    # Test client functions
    try:
        from test_client_functions import run_all_tests as test_client
        results['Client Functions'] = run_test_suite("Client Functions Tests", test_client)
    except Exception as e:
        logger.error(f"Failed to import/run client tests: {e}")
        results['Client Functions'] = False
    
    # Test async client functions
    try:
        from test_async_client_functions import run_all_tests as test_async_client
        results['Async Client Functions'] = run_test_suite("Async Client Functions Tests", test_async_client)
    except Exception as e:
        logger.error(f"Failed to import/run async client tests: {e}")
        results['Async Client Functions'] = False
    
    # Test server functions
    try:
        from test_server_functions import run_all_tests as test_server
        results['Server Functions'] = run_test_suite("Server Functions Tests", test_server)
    except Exception as e:
        logger.error(f"Failed to import/run server tests: {e}")
        results['Server Functions'] = False
    
    # Test maintain_database functions
    try:
        from test_maintain_database_functions import run_all_tests as test_maintain
        results['Maintain Database Functions'] = run_test_suite("Maintain Database Functions Tests", test_maintain)
    except Exception as e:
        logger.error(f"Failed to import/run maintain_database tests: {e}")
        results['Maintain Database Functions'] = False
    
    # Print summary
    logger.info("\n" + "=" * 80)
    logger.info("TEST SUMMARY")
    logger.info("=" * 80)
    
    for test_name, passed in results.items():
        status = "✓ PASSED" if passed else "✗ FAILED"
        logger.info(f"{test_name}: {status}")
    
    total = len(results)
    passed = sum(1 for v in results.values() if v)
    
    logger.info(f"\nTotal: {passed}/{total} test suites passed")
    
    if passed == total:
        logger.info("\n🎉 All tests passed!")
        return 0
    else:
        logger.warning(f"\n⚠ {total - passed} test suite(s) had issues")
        return 1


if __name__ == "__main__":
    sys.exit(main())
