==================================================
Ocean Observatories Initiative Cyberinfrastructure
Integrated Observatory Network (ION)
eoi - External Observatory Integration Modules
==================================================

Get eoi with
::
	git clone git@amoeba.ucsd.edu:eoi.git
	cd eoi


Dependencies
============

eoi requires the following projects:
	lcaarch - git@amoeba.ucsd.edu:lcaarch.git
	ioncore-java - git@amoeba.ucsd.edu:ioncore-java.git
	ooi-netcdf-java - git@amoeba.ucsd.edu:ooi-netcdf-java.git (WARNING - large repository)
	
****These dependencies should be obtained and compiled prior to continuing****


Usage
=====

Start an empty Python Capability Container shell (see lcaarch/Readme.txt for more information):
(from lcaarch/ root directory)

You can of course swap out the host with your own or leave the -h out to use localhost
The sysname needs to be set to "cmueller" - this will be fixed...
::
	bin/start-cc -h amoeba.ucsd.edu -a sysname=cmueller

At the prompt, start the iosptest script:
><>
	from ion.play import iospservice
	iospservice.main(locals())
	
Open a new terminal and navigate to the eoi directory:
(all subsequent steps assume you are in the eoi/ root dir)

Use ant to test the iosp (use "ant -p" to see a list of tasks):
::
	ant fullTest

	

**************************
IVY Instructions
**************************
Notes:
- the contents of "ion-libs" and the associated line in ivysettings.xml are temporary - they will be replaced when the libraries are available on the package server
- zz is because the ooi-netcdf-full lib has conflicting dependencies - will fix after decision on dependency management

Ivy Installation:
- install ivy
	1. download source from:  svn co https://svn.apache.org/repos/asf/ant/ivy/core/trunk ivy
	2. from ivy directory, run "ant jar"
	3. from ivy directory, run "sudo cp build/artifact/jars/ivy.jar /usr/share/ant/lib/ivy.jar"
	**IF that copy doesn't work:
		- put the ivy.jar file (from "ivy/build/artifact/jars/") in /usr/share/ant/java/ant-1.8.1/lib/
			- full install instructions: http://ant.apache.org/ivy/history/2.2.0-rc1/install.html
				- you can find where ant is installed this way: http://stackoverflow.com/questions/4823367/where-is-ant-installed-on-mac-os-x-snow-leopard-by-default
	4. Check that ivy is installed by running "ant" from the ivy/src/example/hello-ivy directory – if it doesn't bomb, it's installed correctly

From eoi-agents-ivy:
- run "ant test-all" --> calls the "resolve" and "compile" targets