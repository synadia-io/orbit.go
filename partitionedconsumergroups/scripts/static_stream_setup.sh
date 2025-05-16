#!/bin/bash
nats stream add foo --subjects="foo.*" --transform-source="foo.*" --transform-destination="{{partition(10,1)}}.foo.{{wildcard(1)}}" --defaults