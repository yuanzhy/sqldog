package com.yuanzhy.sqldog.cli.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Scanner;

/**
 *
 * @author yuanzhy
 * @date 2021-11-05
 */
public class SocketClient {

    public void connect(String host, int port, String username, String password) throws IOException {
        Socket s = new Socket(host, port);
        InputStream is = s.getInputStream();
        OutputStream os = s.getOutputStream();
        BufferedReader bufNet = new BufferedReader(new InputStreamReader(is));
        PrintWriter prtWriter = new PrintWriter(os, true);
        // write username or password
        // TODO
        prtWriter.println(username);
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
