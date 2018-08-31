#!/usr/bin/env bash

cd "$(cd "$(dirname "$0")" && pwd)"
protoc ./*.proto --java_out ../src/main/java

