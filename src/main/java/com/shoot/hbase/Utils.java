package com.shoot.hbase;

import com.shoot.types.Card;
import com.shoot.types.Faul;
import com.shoot.types.Goal;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
public class Utils {
    public static Runtime process = Runtime.getRuntime();

    public static void addCard( Card c) throws IOException {
        try {
            String command = "java AddElement card " + c.player+ " " + c.team + " " + c.color;
            Process proc = Runtime.getRuntime().exec(new String[] { "/bin/sh"//$NON-NLS-1$
                    , "-c", command });//$NON-NLS-1$
            if (proc != null) {
                proc.waitFor();
            }
        } catch (Exception e) {
            //Handle
            return;
        }
    }
    public static void addFaul( Faul f) throws IOException {
        try {
            String command = "java AddElement faul " + f.player+ " " + f.team + " " + f.type;
            Process proc = Runtime.getRuntime().exec(new String[] { "/bin/sh"//$NON-NLS-1$
                    , "-c", command });//$NON-NLS-1$
            if (proc != null) {
                proc.waitFor();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static void addGoal( Goal g) throws IOException {
        try {
            String command = "java AddElement goal " + g.player+ " " + g.team + " " + g.assist;
            Process proc = Runtime.getRuntime().exec(new String[] { "/bin/sh"//$NON-NLS-1$
                    , "-c", command });//$NON-NLS-1$
            if (proc != null) {
                proc.waitFor();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
