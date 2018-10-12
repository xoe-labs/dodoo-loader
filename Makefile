# Init the repository


init: chmod-scripts
	editor hack/variables.ini
	editor hack/boilerplate.readme.credits.txt
	hack/init-repo.sh

sync: chmod-scripts
	hack/sync-with-scaffold.sh

chmod-scripts:
	chmod +x -R hack
