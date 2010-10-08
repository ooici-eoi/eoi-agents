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
::
	bin/start-cc

At the prompt, start the iosptest script:
><>
	from ion.play import iospservice
	iospservice.main(locals())
	
Open a new terminal and navigate to the eoi directory:
(all subsequent steps assume you are in the eoi/ root dir)

Use ant to test the iosp (use "ant -p" to see a list of tasks):
::
	ant fullTest

	
