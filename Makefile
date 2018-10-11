# Init the repository


init: chmod-scripts
	editor hack/variables.ini
	editor hack/boilerplate.readme.credits.txt
	hack/init-repo.sh

sync:
	git pull scaffold master

chmod-scripts:
	chmod +x -R hack
