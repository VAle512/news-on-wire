#!/bin/bash

# Just a simple script to flush any generated files

echo "Removing mySQL database files..."
rm -rfv ./mysqldata/*

echo "Removing website data crawled..."
rm -rfv ./app/data/*

echo "Removing csv..."
rm -rfv ./app/csv/*

echo "Removing debug..."
rm -rfv ./app/debug/*

echo "Removing results..."
rm -rfv ./app/results/*
