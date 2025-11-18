# Integration Test Design for MCP Server DataHub

## Overview

This document describes the design for adding robust integration tests to the MCP Server DataHub project. The goal is to test MCP tools against real DataHub instances (both OSS and Cloud) using a CI/CD matrix strategy.

## Current State

**What we have:**
- Basic unit tests in `tests/test_mcp_server.py`
- Tests connect to a live instance (longtailcompanions)
- Tests require pre-configured credentials via `DATAHUB_GMS_URL` env var
- Tests skip when credentials aren't available
- No automated CI-based integration testing

**Limitations:**
- Tests depend on external instance availability
- No version compatibility testing
- No isolation between test runs
- No testing against DataHub OSS versions

## Proposed Architecture

### 1. Test Matrix Strategy

Test against multiple DataHub configurations using GitHub Actions matrix strategy:

```yaml
strategy:
  matrix:
    include:
      # OSS versions
      - datahub_type: "oss"
        datahub_version: "v0.14.1"
        test_suite: "core"

      - datahub_type: "oss"
        datahub_version: "head"  # Latest master
        test_suite: "core"

      # Cloud instance (using staging)
      - datahub_type: "cloud"
        datahub_env: "staging"
        test_suite: "cloud_features"
```

### 2. Test Suite Organization

```
tests/
├── integration/
│   ├── oss/
│   │   ├── test_basic_operations.py      # Search, get_entity, lineage
│   │   ├── test_queries.py               # Query operations
│   │   └── conftest.py                   # OSS fixtures
│   │
│   ├── cloud/
│   │   ├── test_cloud_features.py        # Cloud-specific features
│   │   ├── test_advanced_search.py       # Semantic search, etc.
│   │   └── conftest.py                   # Cloud fixtures
│   │
│   ├── common/
│   │   └── test_mcp_tools.py             # Tests for both OSS & Cloud
│   │
│   └── conftest.py                        # Shared fixtures
│
├── unit/
│   └── test_mcp_server.py                # Existing unit tests
│
└── conftest.py                            # Top-level fixtures
```

### 3. Test Execution Flow

#### OSS Integration Tests (Run on every PR)

1. **Setup Phase:**
   - Free up disk space on GitHub runner
   - Install Python dependencies
   - Install DataHub CLI (`pip install acryl-datahub`)
   - Start DataHub instance: `datahub docker quickstart --version <VERSION>`
   - Verify instance: `datahub docker check`
   - Ingest sample data: `datahub docker ingest-sample-data`

2. **Test Phase:**
   - Install MCP server dependencies
   - Run integration tests against localhost:8080
   - Test all MCP tools (search, get_entity, lineage, etc.)
   - Validate responses against known sample data

3. **Cleanup Phase:**
   - Stop DataHub containers: `datahub docker nuke`
   - Free disk space

#### Cloud Integration Tests (Run nightly/on-demand)

1. **Setup Phase:**
   - Install Python dependencies
   - Install MCP server dependencies
   - Configure Cloud credentials from GitHub secrets

2. **Test Phase:**
   - Run Cloud-specific tests against staging environment
   - Test Cloud-only features (semantic search, advanced RBAC)
   - Validate against known test data in Cloud instance

3. **Cleanup Phase:**
   - No infrastructure cleanup needed (managed instance)

### 4. Test Fixtures

#### Shared Fixtures (`tests/integration/conftest.py`)

```python
@pytest.fixture(scope="session")
def test_suite():
    """Determine which test suite we're running (oss/cloud)"""
    return os.environ.get("TEST_SUITE", "oss")

@pytest.fixture(scope="session")
def datahub_client(test_suite):
    """Create DataHub client based on environment"""
    gms_url = os.environ.get("DATAHUB_GMS_URL")
    gms_token = os.environ.get("DATAHUB_GMS_TOKEN")

    if not gms_url:
        pytest.skip("DATAHUB_GMS_URL not set")

    config = DataHubClient.Config()
    config.server = gms_url
    if gms_token:
        config.token = gms_token

    return DataHubClient(config=config)
```

#### OSS-Specific Fixtures

```python
@pytest.fixture
def sample_dataset_urn():
    """Return test dataset URN from OSS quickstart sample data"""
    return "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"
```

#### Cloud-Specific Fixtures

```python
@pytest.fixture
def sample_dataset_urn():
    """Return test dataset URN from Cloud staging instance"""
    return "urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.analytics.pet_details,PROD)"
```

### 5. Test Categories

#### Core Tests (Run against OSS & Cloud)

- **Basic Operations:**
  - Search for entities
  - Get entity by URN
  - Get lineage (upstream/downstream)
  - Query entities with filters

- **Validation Tests:**
  - Response schema validation
  - Error handling
  - Rate limiting behavior
  - Authentication/authorization

#### OSS-Specific Tests

- **Sample Data Tests:**
  - Verify sample datasets are searchable
  - Validate lineage from sample data
  - Test against known OSS features

#### Cloud-Specific Tests

- **Advanced Features:**
  - Semantic search
  - Advanced filtering
  - Cloud-only APIs
  - RBAC behavior

### 6. GitHub Actions Workflow

**File: `.github/workflows/integration-tests.yml`**

Key components:

1. **OSS Integration Job:**
   - Matrix across OSS versions
   - Uses `datahub docker quickstart`
   - Runs on every PR and main branch
   - Fast feedback loop (~10-15 minutes)

2. **Cloud Integration Job:**
   - Single cloud staging environment
   - Uses GitHub secrets for credentials
   - Runs nightly or on-demand
   - Doesn't block PRs

3. **Optimizations:**
   - Disk space cleanup (GitHub runners have limited space)
   - Parallel matrix jobs
   - `fail-fast: false` to see all failures
   - Conditional execution (Cloud tests only nightly)

## Implementation Phases

### Phase 1: Basic OSS Integration (MVP)
- [ ] Create test directory structure
- [ ] Add GitHub Actions workflow for OSS testing
- [ ] Implement basic test fixtures
- [ ] Write core tests (search, get_entity)
- [ ] Test against single OSS version (v0.14.1)

### Phase 2: Multi-Version Testing
- [ ] Add matrix strategy for multiple OSS versions
- [ ] Add version-specific test variations
- [ ] Implement compatibility testing

### Phase 3: Cloud Integration
- [ ] Set up Cloud staging credentials
- [ ] Create Cloud-specific test suite
- [ ] Add nightly Cloud test job
- [ ] Implement Cloud feature tests

### Phase 4: Enhancements
- [ ] Add performance benchmarks
- [ ] Implement test data generation
- [ ] Add test coverage reporting
- [ ] Create test documentation

## Benefits

1. **Confidence:** Every PR is validated against real DataHub instances
2. **Compatibility:** Matrix testing catches version-specific issues early
3. **Automation:** No manual testing required for basic validation
4. **Coverage:** Tests both OSS and Cloud scenarios
5. **Fast Feedback:** OSS tests provide quick PR validation
6. **Quality:** Prevents regressions in MCP tool behavior

## Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| GitHub runner disk space | Free up space before starting containers |
| Flaky tests due to timing | Add retries and proper wait conditions |
| Long test duration | Run Cloud tests nightly, not on every PR |
| Credentials exposure | Use GitHub secrets, never commit credentials |
| Test data drift | Use deterministic sample data for OSS |

## Open Questions

1. **Cloud Environment:** Which Cloud instance should be used for testing? (Staging/Dev)
2. **Test Data:** Do we need to ingest specific test data for Cloud tests?
3. **Secrets Management:** Who will set up GitHub secrets for Cloud credentials?
4. **Version Coverage:** Which OSS versions should we test? (Latest 2 releases + head?)
5. **Test Frequency:** Should Cloud tests run on every PR or only nightly?

## References

- DataHub OSS CI: https://github.com/datahub-project/datahub/blob/master/.github/workflows/docker-unified.yml
- DataHub Quickstart: https://datahubproject.io/docs/quickstart
- MCP Server Tests: https://github.com/acryldata/mcp-server-datahub/blob/main/tests/test_mcp_server.py
- GitHub Actions Matrix: https://docs.github.com/en/actions/using-jobs/using-a-matrix-for-your-jobs

## Next Steps

1. Get approval on this design
2. Set up necessary GitHub secrets
3. Implement Phase 1 (Basic OSS Integration)
4. Iterate based on feedback
