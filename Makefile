immysql:
	importer -config ./mysql.toml

imtidb:
	importer -config ./tidb.toml

install:
	go install

