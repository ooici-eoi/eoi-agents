---
**Ocean Observatories Initiative Cyberinfrastructure**  
**Integrated Observatory Network (ION)**  
**eoi-agents** - External Observatory Integration (EOI) Dataset Agent Framework (DAF)

---

# Description
*need concise description of project*

#Source

Obtain the THREDDS project by running:  

    git clone -b develop git@github.com:ooici-eoi/eoi-agents.git
    cd eoi-agents

#Targets
This project uses Apache Ant for resolving, compiling, and building.  A complete list of the available targets can be obtained by running:

    ant -p

There are two "sets" of targets that are very similar, but serve quite different purposes: **Standard Build Targets** and **Development Build Targets**

##When to use Standard vs Development
The **Standard Build Targets** should be used in most cases by most developers.  Using the **Standard** targets ensures that you're working with the latest "stable" releases of the dependencies.

The **Development Build Targets** should only be used when the underlying OOI-CI project dependencies (e.g. ioncore-java) are under active development and changing rapidly (thereby making full "stable" releases cumbersome).

**NOTE**: When releasing the library using the release scripts in *ion-integration*, the **Standard** targets are used.

##Standard Build  Targets
The standard build pathways utilize the *ivy.xml* file to resolve the project dependencies from a remote repository.
###Resolve  
Resolves the project dependencies and copies the jar files to the *lib* directory

    ant resolve

###Compile

    ant compile


###Distribution
Resulting JAR file in dist/lib  

    ant dist

##Development Build Targets
The development build also utilizes the *ivy.xml* for resolving the third party dependencies, but then uses the projects listed in the **dev-project.list** property of *build.properties* to resolve the "local" projects (e.g. ioncore-java).  This allows changes to dependent projects to be tested without releasing versions of those dependent projects.  

The **dev-project.list** is a semi-colon separated list of relative project paths.  For example, the following would result in both the *ioncore-java* and *ion-object-definitions* projects (located in the same directory as *eoi-agents*) being used instead of the compiled libraries in the remote package repository:

    dev-project.list=../ioncore-java;../ion-object-definitions

###Resolve
Resolves the project dependencies and copies the jar files to the *lib* directory

    ant resolve-dev

###Compile

    ant compile-dev

###Distribution

    ant dist-dev

##IDE Setup
1. From 'eoi-agents', run:  

        ant resolve  

* Make a "existing source" (or similar) project in your IDE of choice and select the src directory of the project.  

* Put all of the jar files in the "lib" directory on the project classpath in your IDE.  
** Note - when you "ant clean" the lib directory is wiped out.  If you discover your project not compiling, make sure
there's a lib directory!
