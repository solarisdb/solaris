![CI](https://github.com/solarisdb/solaris/golibs/actions/workflows/ci.yaml/badge.svg)[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/solarisdb/solaris/golibs/blob/main/LICENSE)
# golibs
The repository contains utility code that can be used by applications written in Golang. Can be called 'common utils'.

## Intention
In many services, we use the same things to instrument and support them, e.g., logging, context management, transformations, syncs, etc. We collect them in one place to avoid copy-paste and form a common place for the code that multiple services can use.

## Rules
- the folders in the root have names of the class of objects or the functionality it works with: `contexts`, `sync`, `logging` etc.
- all commits should be reviewed.
- changes in the utilities should be incremental and backward compatible. Use a new name/package path if you add functionality incompatible with the existing one. 

## License
This project is licensed under the Apache Version 2.0 License - see the [LICENSE](LICENSE) file for details

## Acknowledgments
* GoLand IDE by [JetBrains](https://www.jetbrains.com/go/) is used for the code development
