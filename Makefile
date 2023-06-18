align:
	betteralign -apply ./...
t:
	go mod tidy
v:
	go mod vendor
gv:
	git add vendor/
tv: t v gv
