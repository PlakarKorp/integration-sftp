GO=go

all: build

build:
	${GO} build -v -o sftpImporter ./plugin/importer
	${GO} build -v -o sftpExporter ./plugin/exporter
	${GO} build -v -o sftpStorage ./plugin/storage

clean:
	rm -f sftpImporter sftpExporter sftpStorage sftp-*.ptar
