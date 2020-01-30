#!/usr/bin/env bash
mkdir dep_packages
pip install -r requirements.txt --target ./dep_packages
cd dep_packages
zip -9mrv dep_packages.zip .
mv dep_packages.zip ..
rm -rf dep_packages
