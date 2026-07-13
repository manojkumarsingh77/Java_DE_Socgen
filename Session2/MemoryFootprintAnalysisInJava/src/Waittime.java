long start = System.currentTimeMillis();

// waiting time
long waitStart = System.currentTimeMillis();
Thread.sleep(200);   // simulate DB/API wait
long waitEnd = System.currentTimeMillis();

// compute time
long computeStart = System.currentTimeMillis();

double sum = 0;
for(int i=0;i<1000000;i++){
sum += Math.sqrt(i);
}

long computeEnd = System.currentTimeMillis();

long waitTime = waitEnd - waitStart;
long computeTime = computeEnd - computeStart;

double wcRatio = (double) waitTime / computeTime;

System.out.println("Wait Time: " + waitTime + " ms");
System.out.println("Compute Time: " + computeTime + " ms");
System.out.println("W/C Ratio: " + wcRatio);



java -XX:StartFlightRecording=filename=app.jfr,duration=60s -jar app.jar