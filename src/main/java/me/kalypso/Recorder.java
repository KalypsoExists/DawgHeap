package me.kalypso;

public class Recorder {

    private long[] array = new long[1024];
    private int bar = -1;

    public Recorder() {}

    public static long quickStart() {
        return System.nanoTime();
    }

    public static void quickEnd(long start) {
        long now = System.nanoTime();
        System.out.println("Quack "+asMillis(new StringBuilder(), now-start)+ " ms");
    }

    public static void quickEndPrint(long start) {
        long now = System.nanoTime();
        System.out.println(asMillis(new StringBuilder(), now-start) + " ms");
    }

    public void reset() {
        array = new long[1024];
        bar = 0;
    }

    public int start() {
        bar++;
        int index = bar;
        array[index] = System.nanoTime();
        return index;
    }

    public void end(int index) {
        long now = System.nanoTime();
        array[index] = now - array[index];
    }

    public String[] flush() {

        int count = 0;
        while (count < array.length && array[count] != 0) {
            count++;
        }

        String[] output = new String[count];
        StringBuilder sb = new StringBuilder(32);

        for (int i = 0; i < count; i++) {
            sb.setLength(0);
            output[i] = asMillis(sb, array[i]);
        }

        reset();
        return output;
    }

    public static String asMillis(StringBuilder sb, long nano) {
        String s = Long.toString(nano);
        int len = s.length();

        if (len > 6) {
            sb.append(s).insert(len - 6, '.');
        } else {
            sb.append("0.");
            for (int j = 0; j < 6 - len; j++) sb.append('0');
            sb.append(s);
        }

        int last = sb.length() - 1;
        while (last >= 0 && sb.charAt(last) == '0') {
            last--;
        }
        if (last >= 0 && sb.charAt(last) == '.') {
            last--;
        }
        return sb.substring(0, last + 1);
    }

}
