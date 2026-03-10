#!/bin/bash
nats bench js pub sync foo --multisubject --sleep 10ms --multisubjectmax 100 --stream foo
