# Changelog

## [1.0.0](https://github.com/equinor/dsis-python-client/compare/v0.5.0...v1.0.0) (2025-12-23)


### ⚠ BREAKING CHANGES

* **dsis:** rename 'field' parameter to 'project' across API, docs and tests ([#49](https://github.com/equinor/dsis-python-client/issues/49))

### Features

* fix types to follow mypy standards ([#46](https://github.com/equinor/dsis-python-client/issues/46)) ([b7f1889](https://github.com/equinor/dsis-python-client/commit/b7f18898a1f3869ee69f5aea0f5edaf7e1d6cb10))


### Code Refactoring

* **dsis:** rename 'field' parameter to 'project' across API, docs and tests ([#49](https://github.com/equinor/dsis-python-client/issues/49)) ([d3791e6](https://github.com/equinor/dsis-python-client/commit/d3791e60db7a0a2a63a5f1734ec9470dfca974f2))

## [0.5.0](https://github.com/equinor/dsis-python-client/compare/v0.4.1...v0.5.0) (2025-12-10)


### Features

* support ptotobuf in dsis client ([#39](https://github.com/equinor/dsis-python-client/issues/39)) ([1be337a](https://github.com/equinor/dsis-python-client/commit/1be337ab8c43322aa29e55c202500d7b237dd28c))

## [0.4.1](https://github.com/equinor/dsis-python-client/compare/v0.4.0...v0.4.1) (2025-11-13)


### Bug Fixes

* update required Python version to 3.11 in pyproject.toml ([#34](https://github.com/equinor/dsis-python-client/issues/34)) ([d325265](https://github.com/equinor/dsis-python-client/commit/d3252658240d9033d21bf8b1f35b49ba3c412012))

## [0.4.0](https://github.com/equinor/dsis-python-client/compare/v0.3.0...v0.4.0) (2025-11-12)


### Features

* make json format optional in stead of a forced default ([#32](https://github.com/equinor/dsis-python-client/issues/32)) ([735f490](https://github.com/equinor/dsis-python-client/commit/735f490ffa64e0985068b79ecb8f709296cbce00))

## [0.3.0](https://github.com/equinor/dsis-python-client/compare/v0.2.1...v0.3.0) (2025-11-10)


### Features

* add automatic pagination support to DSIS API client ([#29](https://github.com/equinor/dsis-python-client/issues/29)) ([c4e5766](https://github.com/equinor/dsis-python-client/commit/c4e5766e7cb22cee2f5deea4a64ec67c9bfcf725)), closes [#28](https://github.com/equinor/dsis-python-client/issues/28)

## [0.2.1](https://github.com/equinor/dsis-python-client/compare/v0.2.0...v0.2.1) (2025-11-04)


### Bug Fixes

* correct the full form for DSIS in readme ([#22](https://github.com/equinor/dsis-python-client/issues/22)) ([011a5f6](https://github.com/equinor/dsis-python-client/commit/011a5f6140506be8f578beece9cb783f6640d154))

## [0.2.0](https://github.com/equinor/dsis-python-client/compare/v0.1.4...v0.2.0) (2025-11-04)


### Features

* dsis client setup and query builder ([#21](https://github.com/equinor/dsis-python-client/issues/21)) ([7342725](https://github.com/equinor/dsis-python-client/commit/73427254e2288a76e5f3d000cd86445639e949cc))
* Implement query builder for DSIS OData API ([7342725](https://github.com/equinor/dsis-python-client/commit/73427254e2288a76e5f3d000cd86445639e949cc))


### Documentation

* add changelog tab ([#14](https://github.com/equinor/dsis-python-client/issues/14)) ([0c23e6d](https://github.com/equinor/dsis-python-client/commit/0c23e6d3cb3d10f2ef928775b4879215690c661e))
* add home page ([#17](https://github.com/equinor/dsis-python-client/issues/17)) ([1638ce6](https://github.com/equinor/dsis-python-client/commit/1638ce6d4792a59c5f317d9a231c3a7bd2b9aad8))

## [0.1.4](https://github.com/equinor/dsis-python-client/compare/v0.1.3...v0.1.4) (2025-10-15)


### Documentation

* add absolute links for contributing guidelines and license ([d1b11ad](https://github.com/equinor/dsis-python-client/commit/d1b11adfbe850264346cbe6591d1157c8496e2ca))


### Continuous Integration

* build using reusable workflow ([56b2dfb](https://github.com/equinor/dsis-python-client/commit/56b2dfbe6b35513e66beef0a0fc6990a12251f93))

## [0.1.3](https://github.com/equinor/dsis-python-client/compare/v0.1.2...v0.1.3) (2025-10-15)


### Continuous Integration

* run publish to PyPI from caller workflow ([d65e88b](https://github.com/equinor/dsis-python-client/commit/d65e88b19a654e429597d784f0fa78c280e873fc))

## [0.1.2](https://github.com/equinor/dsis-python-client/compare/v0.1.1...v0.1.2) (2025-10-14)


### Continuous Integration

* publish to PyPI on workflow call ([945cce6](https://github.com/equinor/dsis-python-client/commit/945cce6ec6b5587b095a28f605395a86d3657dd9))

## [0.1.1](https://github.com/equinor/dsis-python-client/compare/v0.1.0...v0.1.1) (2025-10-14)


### Miscellaneous Chores

* trigger release ([3544d96](https://github.com/equinor/dsis-python-client/commit/3544d96cb3e0c43506e52c1a3df184f41128f481))

## 0.1.0 (2025-10-14)


### Documentation

* add code of conduct ([77f7dad](https://github.com/equinor/dsis-python-client/commit/77f7dad7d53706f7dc11f2c2833ac7053237ae82))
* add contributing guidelines ([044d116](https://github.com/equinor/dsis-python-client/commit/044d116d9b1835e20aa385bf77d710109e877ea9))
* add initial MkDocs files ([b164722](https://github.com/equinor/dsis-python-client/commit/b164722dd9b9ce25710cdc7e6ac78e3c98aaf814))
* add installation instructions ([e5df746](https://github.com/equinor/dsis-python-client/commit/e5df746cd64c2ddee3c5b88f5672b543016057b7))
* add license section in README ([b62b248](https://github.com/equinor/dsis-python-client/commit/b62b2488f57ced515bd2e1034836132d5e921951))
* add security policy ([ce7066c](https://github.com/equinor/dsis-python-client/commit/ce7066c3872622a062644e18e44f54570897ad3f))
