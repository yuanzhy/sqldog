package com.yuanzhy.sqldog.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Scanner;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/31
 */
public class BioClientTest {

    public static void main(String[] args) throws IOException {
        Socket s = new Socket("127.0.0.1", 2345);
        InputStream is = s.getInputStream();
        OutputStream os = s.getOutputStream();
        BufferedReader bufNet = new BufferedReader(new InputStreamReader(is));
        PrintWriter prtWriter = new PrintWriter(os, true);
        Scanner scanner = new Scanner(System.in);
        while (true) {
            String input = scanner.nextLine();
            prtWriter.println(input);
            if (input.equalsIgnoreCase("quit")) {
                break;
            }
            System.out.println(bufNet.readLine());
        }
        prtWriter.close();
        bufNet.close();
        scanner.close();
        s.close();
    }
}
