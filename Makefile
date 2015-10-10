SRC			= *.class
JAVAFILES	= *.java
TARGET		= SimilarNetflixUsers.jar

JC			= javac
JAR			= jar
JFLAGS		= -cf
SLURM		= similarNetflixUsers.slurm
SBATCH		= sbatch
TEMPOUT		= temp
TEMPOUT2	= temp2
OUTPUTPATH	= output


$(TARGET): $(SRC)
	$(JC) $(JAVAFILES)
	$(JAR) $(JFLAGS) $(TARGET) $(SRC)

run: $(TARGET)
	$(SBATCH) $(SLURM)

clean:
	rm -rf ./$(TARGET)

clean-output:
	rm -rf ./$(OUTPUTPATH)

clean-all:
	rm -rf ./$(TARGET)
	rm -rf ./$(TEMPOUT)
	rm -rf ./$(TEMPOUT2)
	rm -rf ./$(OUTPUTPATH)