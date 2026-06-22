# Changelog

## [0.7.0](https://github.com/iExec-Nox/nox-ingestor-replayer/compare/v0.6.0...v0.7.0) (2026-06-22)


### Features

* add boolean operators ([#17](https://github.com/iExec-Nox/nox-ingestor-replayer/issues/17)) ([8ac1832](https://github.com/iExec-Nox/nox-ingestor-replayer/commit/8ac1832b8099ffa43e0944dd14ab6226c81a8bb9))
* add docker release gha ([#29](https://github.com/iExec-Nox/nox-ingestor-replayer/issues/29)) ([d6cda7e](https://github.com/iExec-Nox/nox-ingestor-replayer/commit/d6cda7e81a5ddc38c34a047b4b7360de867eb966))
* add minimal TCP server for health checks ([#23](https://github.com/iExec-Nox/nox-ingestor-replayer/issues/23)) ([a6803af](https://github.com/iExec-Nox/nox-ingestor-replayer/commit/a6803af2ef279835c6d38af26a5b916cd6832cb8))
* add NATS JetStream publisher for transaction events ([#11](https://github.com/iExec-Nox/nox-ingestor-replayer/issues/11)) ([a9181e1](https://github.com/iExec-Nox/nox-ingestor-replayer/commit/a9181e155afdef1f9d9b1279ebb767139dde36b9))
* add persistent state store for reliable block tracking ([#6](https://github.com/iExec-Nox/nox-ingestor-replayer/issues/6)) ([efeceb9](https://github.com/iExec-Nox/nox-ingestor-replayer/commit/efeceb98c4ba35b3b230e44ffeaed266deb692a2))
* add safe arithmetic operations ([#14](https://github.com/iExec-Nox/nox-ingestor-replayer/issues/14)) ([08c3481](https://github.com/iExec-Nox/nox-ingestor-replayer/commit/08c34811119d5e52de85977409ff6298f2c38aa1))
* add transfer, mint, and burn operations ([#19](https://github.com/iExec-Nox/nox-ingestor-replayer/issues/19)) ([951f15a](https://github.com/iExec-Nox/nox-ingestor-replayer/commit/951f15a0bee0a79e2f38d759fd9ab0c6a3673295))
* add typed event payload structures ([#10](https://github.com/iExec-Nox/nox-ingestor-replayer/issues/10)) ([e9d489c](https://github.com/iExec-Nox/nox-ingestor-replayer/commit/e9d489c2f4828d4bb79d4717d341c9dc31126206))
* add WrapPublicHandle support ([#22](https://github.com/iExec-Nox/nox-ingestor-replayer/issues/22)) ([9db46c3](https://github.com/iExec-Nox/nox-ingestor-replayer/commit/9db46c3e8f5a7bd7bf454fedb2a9b7fbd7497225))
* block reader ([#8](https://github.com/iExec-Nox/nox-ingestor-replayer/issues/8)) ([e8564f4](https://github.com/iExec-Nox/nox-ingestor-replayer/commit/e8564f4e7d8d67335d6ffa86f8c0c3256587c681))
* expose first Nox metrics to Prometheus ([#34](https://github.com/iExec-Nox/nox-ingestor-replayer/issues/34)) ([b721a52](https://github.com/iExec-Nox/nox-ingestor-replayer/commit/b721a52bbc4704bcb9ce98fcdbd5116c129523f9))
* expose Prometheus metrics on /metrics ([#26](https://github.com/iExec-Nox/nox-ingestor-replayer/issues/26)) ([4babf54](https://github.com/iExec-Nox/nox-ingestor-replayer/commit/4babf546fc0b4a7303db32423a6b0dc40c612d34))
* implement graceful shutdown with signal handling ([#9](https://github.com/iExec-Nox/nox-ingestor-replayer/issues/9)) ([be9fc86](https://github.com/iExec-Nox/nox-ingestor-replayer/commit/be9fc86f3003cba1f8678105ac147d10461e9b2e))
* init configuration and application ([#4](https://github.com/iExec-Nox/nox-ingestor-replayer/issues/4)) ([a7aec97](https://github.com/iExec-Nox/nox-ingestor-replayer/commit/a7aec97583971c80c3b35d94e679961e65b2e9d4))
* initialize chain client and events parser ([#5](https://github.com/iExec-Nox/nox-ingestor-replayer/issues/5)) ([97196e0](https://github.com/iExec-Nox/nox-ingestor-replayer/commit/97196e0d67e2636722d7a3db87eabf995e20f62d))
* initialize project ([#1](https://github.com/iExec-Nox/nox-ingestor-replayer/issues/1)) ([a915db2](https://github.com/iExec-Nox/nox-ingestor-replayer/commit/a915db2f171a4ea21e239926e8bcd159afe6f935))
* nats resilience management ([#13](https://github.com/iExec-Nox/nox-ingestor-replayer/issues/13)) ([b38f6ee](https://github.com/iExec-Nox/nox-ingestor-replayer/commit/b38f6ee9e76ae54c05c705e3a6899482cbb1c2fd))
* support NATS cluster interactions and resilience ([#36](https://github.com/iExec-Nox/nox-ingestor-replayer/issues/36)) ([06bc84b](https://github.com/iExec-Nox/nox-ingestor-replayer/commit/06bc84b161b93d6932e4d54cfffc444d8ec43419))
* use Address type in config ([#7](https://github.com/iExec-Nox/nox-ingestor-replayer/issues/7)) ([debe1ec](https://github.com/iExec-Nox/nox-ingestor-replayer/commit/debe1ecdfe7f4f1b54d9d249dc10ade0d713897c))
* use duration in config instead of u64 ([#12](https://github.com/iExec-Nox/nox-ingestor-replayer/issues/12)) ([47984fa](https://github.com/iExec-Nox/nox-ingestor-replayer/commit/47984fa9b067555b2ba992745c9caba9e6c29c3a))


### Bug Fixes

* inject release please secret instead of inherit ([#2](https://github.com/iExec-Nox/nox-ingestor-replayer/issues/2)) ([983d9f5](https://github.com/iExec-Nox/nox-ingestor-replayer/commit/983d9f5744695b87069b2ee0c5b5c513a6fc9d25))
* normalize certificats ([#39](https://github.com/iExec-Nox/nox-ingestor-replayer/issues/39)) ([19f401a](https://github.com/iExec-Nox/nox-ingestor-replayer/commit/19f401abf7c1f661a737c3f326d27e7bc5f7440e))
* PlaintextToEncrypted event has been removed from NoxCompute ([#31](https://github.com/iExec-Nox/nox-ingestor-replayer/issues/31)) ([eb06122](https://github.com/iExec-Nox/nox-ingestor-replayer/commit/eb0612237d14707e84a410e443665cc8d1896456))
* rename state_file to state_path in config ([#20](https://github.com/iExec-Nox/nox-ingestor-replayer/issues/20)) ([9702463](https://github.com/iExec-Nox/nox-ingestor-replayer/commit/970246339d7fc8672f89f92e9ac61f3939d9e75d))

## [0.6.0](https://github.com/iExec-Nox/nox-ingestor/compare/v0.5.0...v0.6.0) (2026-06-02)


### Features

* expose first Nox metrics to Prometheus ([#34](https://github.com/iExec-Nox/nox-ingestor/issues/34)) ([b721a52](https://github.com/iExec-Nox/nox-ingestor/commit/b721a52bbc4704bcb9ce98fcdbd5116c129523f9))
* support NATS cluster interactions and resilience ([#36](https://github.com/iExec-Nox/nox-ingestor/issues/36)) ([06bc84b](https://github.com/iExec-Nox/nox-ingestor/commit/06bc84b161b93d6932e4d54cfffc444d8ec43419))


### Bug Fixes

* normalize certificats ([#39](https://github.com/iExec-Nox/nox-ingestor/issues/39)) ([19f401a](https://github.com/iExec-Nox/nox-ingestor/commit/19f401abf7c1f661a737c3f326d27e7bc5f7440e))
* PlaintextToEncrypted event has been removed from NoxCompute ([#31](https://github.com/iExec-Nox/nox-ingestor/issues/31)) ([eb06122](https://github.com/iExec-Nox/nox-ingestor/commit/eb0612237d14707e84a410e443665cc8d1896456))

## [0.5.0](https://github.com/iExec-Nox/nox-ingestor/compare/v0.4.0...v0.5.0) (2026-03-27)


### Features

* add boolean operators ([#17](https://github.com/iExec-Nox/nox-ingestor/issues/17)) ([8ac1832](https://github.com/iExec-Nox/nox-ingestor/commit/8ac1832b8099ffa43e0944dd14ab6226c81a8bb9))
* add docker release gha ([#29](https://github.com/iExec-Nox/nox-ingestor/issues/29)) ([d6cda7e](https://github.com/iExec-Nox/nox-ingestor/commit/d6cda7e81a5ddc38c34a047b4b7360de867eb966))
* add minimal TCP server for health checks ([#23](https://github.com/iExec-Nox/nox-ingestor/issues/23)) ([a6803af](https://github.com/iExec-Nox/nox-ingestor/commit/a6803af2ef279835c6d38af26a5b916cd6832cb8))
* add NATS JetStream publisher for transaction events ([#11](https://github.com/iExec-Nox/nox-ingestor/issues/11)) ([a9181e1](https://github.com/iExec-Nox/nox-ingestor/commit/a9181e155afdef1f9d9b1279ebb767139dde36b9))
* add persistent state store for reliable block tracking ([#6](https://github.com/iExec-Nox/nox-ingestor/issues/6)) ([efeceb9](https://github.com/iExec-Nox/nox-ingestor/commit/efeceb98c4ba35b3b230e44ffeaed266deb692a2))
* add safe arithmetic operations ([#14](https://github.com/iExec-Nox/nox-ingestor/issues/14)) ([08c3481](https://github.com/iExec-Nox/nox-ingestor/commit/08c34811119d5e52de85977409ff6298f2c38aa1))
* add transfer, mint, and burn operations ([#19](https://github.com/iExec-Nox/nox-ingestor/issues/19)) ([951f15a](https://github.com/iExec-Nox/nox-ingestor/commit/951f15a0bee0a79e2f38d759fd9ab0c6a3673295))
* add typed event payload structures ([#10](https://github.com/iExec-Nox/nox-ingestor/issues/10)) ([e9d489c](https://github.com/iExec-Nox/nox-ingestor/commit/e9d489c2f4828d4bb79d4717d341c9dc31126206))
* add WrapPublicHandle support ([#22](https://github.com/iExec-Nox/nox-ingestor/issues/22)) ([9db46c3](https://github.com/iExec-Nox/nox-ingestor/commit/9db46c3e8f5a7bd7bf454fedb2a9b7fbd7497225))
* block reader ([#8](https://github.com/iExec-Nox/nox-ingestor/issues/8)) ([e8564f4](https://github.com/iExec-Nox/nox-ingestor/commit/e8564f4e7d8d67335d6ffa86f8c0c3256587c681))
* expose Prometheus metrics on /metrics ([#26](https://github.com/iExec-Nox/nox-ingestor/issues/26)) ([4babf54](https://github.com/iExec-Nox/nox-ingestor/commit/4babf546fc0b4a7303db32423a6b0dc40c612d34))
* implement graceful shutdown with signal handling ([#9](https://github.com/iExec-Nox/nox-ingestor/issues/9)) ([be9fc86](https://github.com/iExec-Nox/nox-ingestor/commit/be9fc86f3003cba1f8678105ac147d10461e9b2e))
* init configuration and application ([#4](https://github.com/iExec-Nox/nox-ingestor/issues/4)) ([a7aec97](https://github.com/iExec-Nox/nox-ingestor/commit/a7aec97583971c80c3b35d94e679961e65b2e9d4))
* initialize chain client and events parser ([#5](https://github.com/iExec-Nox/nox-ingestor/issues/5)) ([97196e0](https://github.com/iExec-Nox/nox-ingestor/commit/97196e0d67e2636722d7a3db87eabf995e20f62d))
* initialize project ([#1](https://github.com/iExec-Nox/nox-ingestor/issues/1)) ([a915db2](https://github.com/iExec-Nox/nox-ingestor/commit/a915db2f171a4ea21e239926e8bcd159afe6f935))
* nats resilience management ([#13](https://github.com/iExec-Nox/nox-ingestor/issues/13)) ([b38f6ee](https://github.com/iExec-Nox/nox-ingestor/commit/b38f6ee9e76ae54c05c705e3a6899482cbb1c2fd))
* use Address type in config ([#7](https://github.com/iExec-Nox/nox-ingestor/issues/7)) ([debe1ec](https://github.com/iExec-Nox/nox-ingestor/commit/debe1ecdfe7f4f1b54d9d249dc10ade0d713897c))
* use duration in config instead of u64 ([#12](https://github.com/iExec-Nox/nox-ingestor/issues/12)) ([47984fa](https://github.com/iExec-Nox/nox-ingestor/commit/47984fa9b067555b2ba992745c9caba9e6c29c3a))


### Bug Fixes

* inject release please secret instead of inherit ([#2](https://github.com/iExec-Nox/nox-ingestor/issues/2)) ([983d9f5](https://github.com/iExec-Nox/nox-ingestor/commit/983d9f5744695b87069b2ee0c5b5c513a6fc9d25))
* rename state_file to state_path in config ([#20](https://github.com/iExec-Nox/nox-ingestor/issues/20)) ([9702463](https://github.com/iExec-Nox/nox-ingestor/commit/970246339d7fc8672f89f92e9ac61f3939d9e75d))
