package utils;

import java.util.Random;

public class Counter {
	public static void main(String[] args) {
		Random r = new Random();
		for (int i = 1; i < 1001; i++) {
			int a = r.nextInt(1000);
			System.out.println(a);
		}
	}
}
