all the files are an amalgamation of before I moved to the git repo.
the one with the normal `.go` extention is the final one, it got the best result locall on my laptop and on the "resourceful" environment I tested it on with 5.515 seconds time.
the test was done using the time command in linux and it was ran using a container image in k8s with 30 vCPU and 40GB RAM,
though didnt use most of the RAM since the code pretty much loads the text file almost directly throgh the disk cache to the CPU since it's created in the script I included in the container image right  before running the benchmark