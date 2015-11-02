TEST_IMG=debs15-storm-test
MAIN_CLASS=Main

build_run:
	# building with gradle wrapper
	./gradlew build
	# running with docker
	sleep 2
	docker build -t $(TEST_IMG) .
	docker run --rm $(TEST_IMG) $(MAIN_CLASS)
