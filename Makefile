TEST_IMG=debs15-storm-test
MAIN_CLASS=Main

build_run:
	# building with gradle wrapper
	./gradlew build
	# running with docker
	sleep 2
	docker build -t $(TEST_IMG) .
	docker rm -f debs15-running; true
	docker run --name debs15-running $(TEST_IMG) $(MAIN_CLASS)
	docker cp debs15-running:/rankings.output .
	docker rm debs15-running
