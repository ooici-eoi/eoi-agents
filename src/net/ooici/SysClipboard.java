/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package net.ooici;

import java.awt.Toolkit;
import java.awt.datatransfer.Clipboard;
import java.awt.datatransfer.ClipboardOwner;
import java.awt.datatransfer.DataFlavor;
import java.awt.datatransfer.StringSelection;
import java.awt.datatransfer.Transferable;
import java.awt.datatransfer.UnsupportedFlavorException;
import java.io.IOException;

/**
 * Static utility class for copying and pasting to the system clipboard
 *
 *
 * @author cmueller
 */
public class SysClipboard implements ClipboardOwner {
    
    private SysClipboard(){}

    /**
     * "PASTE" - Retrieve the string on the clipboard (the top one if it's a stack)
     * <p>
     *
     * Returns an empty string if there is nothing on the clipboard or the item is not a <code>String</code>
     *
     * @return The string on the clipboard
     */
    public static String pasteString() {
        String result = "";
        Transferable contents = Toolkit.getDefaultToolkit().getSystemClipboard().getContents(null);
        boolean hasTransferableText = (contents != null) && contents.isDataFlavorSupported(DataFlavor.stringFlavor);
        if (hasTransferableText) {
            try {
                result = (String) contents.getTransferData(DataFlavor.stringFlavor);
            } catch (UnsupportedFlavorException ex) {
                //highly unlikely since we are using a standard DataFlavor
                System.out.println(ex);
                ex.printStackTrace();
            } catch (IOException ex) {
                System.out.println(ex);
                ex.printStackTrace();
            }
        }
        return result;
    }

    /**
     * "COPY" - Puts passed string on the clipboard
     *
     * @param toCopy The string to copy
     */
    public static void copyString(String toCopy) {
        Toolkit.getDefaultToolkit().getSystemClipboard().setContents(new StringSelection( toCopy ), null);
    }

    @Override
    public void lostOwnership(Clipboard clipboard, Transferable contents) {
        //None for system
    }

    public static void main(String[] args) {
        /* get the string on the top of the clipboard stack, */
        System.out.println(SysClipboard.pasteString());
        /* put a string at the top of the clipboard stack, */
        SysClipboard.copyString("I was pasted!");
        /* get the string on the top of the clipboard stack again! */
        System.out.println(SysClipboard.pasteString());
    }
}
