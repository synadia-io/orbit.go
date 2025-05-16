#!/bin/bash
nats bench  pub foo --multisubject --sleep 10ms --multisubjectmax 100
