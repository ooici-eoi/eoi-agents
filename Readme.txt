==================================================
Ocean Observatories Initiative Cyberinfrastructure
Integrated Observatory Network (ION)
eoi-agents - External Observatory Integration DatasetAgent Implementation
==================================================

Introduction
============
**** Need blurb ****


Source
======
Obtain the eoi-agents project by running:
::
	git clone git@github.com:ooici-eoi/eoi-agents

NOTE: Unless otherwise noted - all commands should be run from the "eoi-agents" directory
::
	cd eoi-agents


Dependencies
============
Dependencies are managed using Apache Ivy.  If you haven't installed Ivy, please refer to the "Installing Ivy" section below.

To resolve (process and download) dependencies run:
::
	ant resolve


Build.xml
========
All tasks are performed with ant, and are run with:
::
	ant <target>

The ant targets for this project are:

clean            --> Clean the project
clean-ivy-cache  --> Clean the ivy cache
compile          --> Compile the project
deep-clean       --> Cleans both this directory and the ivy cache
dist             --> Package Distribution
javadoc          --> Generate Javadoc
report-deps      --> Generates a report of dependencies
resolve          --> Retreive dependencies with ivy
test-all         --> Runs all tests
test-ncagent     --> Runs a canned netcdf test
test-sos         --> Runs a canned sos test
test-usgs        --> Runs a canned usgs test


Ivy Installation
================
**Full install instructions: http://ant.apache.org/ivy/history/2.2.0-rc1/install.html

1. Download Apache Ivy (OOICI hosted) from: http://ooici.net/packages/ivy.jar

2. Copy/move the ivy.jar to the "lib" directory of your ant installation:
	Mac OSX: usually - /usr/share/ant/lib/
	Linux/Windows: wherever you have ant installed (check "%ANT_HOME%" if you're not sure)

3. To verify the installation run (from eoi-agents directory):
::
	ant test-all 