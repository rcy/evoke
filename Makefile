test:
	go test ./...

release:
	$(if $(TAG),,$(error TAG is not defined))
	git diff-index --quiet HEAD -- # stop if tree is not clean
	git merge-base --is-ancestor HEAD origin/main # stop if HEAD is not pushed
	git tag ${TAG}
	git push origin ${TAG}

tags:
	git tag --list | tail # show the latest tags
