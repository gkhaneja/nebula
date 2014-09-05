package edu.illinois.cs.srg.sim.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Created by gourav on 9/4/14.
 */
public class GoogleTraceIterator implements Iterator {
    private static final Logger LOG = LoggerFactory.getLogger(GoogleTraceIterator.class);

    private String directory;
    private String[] files;
    private int currentFile;
    private String lastLine;
    private BufferedReader reader;

    public GoogleTraceIterator(String directory) throws IllegalArgumentException {
        this.directory = directory;
        File home = new File(directory);
        if (!home.exists() || !home.canRead() || !home.isDirectory()) {
            throw new IllegalArgumentException(directory +
                    " is not a valid directory path with correct permissions");
        }
        this.files = home.list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                if(name.matches("part-[0-9]*-of-[0-9]*.csv")) {
                    return true;
                }
                return false;
            }
        });
        Arrays.sort(files);
        this.currentFile = -1;
        this.lastLine = null;
        nextFile();
    }

    @Override
    public boolean hasNext() {
        if (this.lastLine != null) {
            return true;
        }
        while (this.currentFile < files.length) {
            try {
                this.lastLine = reader.readLine();
                if (this.lastLine != null) {
                    return true;
                } else {
                    nextFile();
                }
            } catch (IOException e) {
                LOG.error("Unable to read " + this.directory + "/" + files[this.currentFile] +
                        " Skipping to next file.", e);
                return false;
            } catch (NoSuchElementException e) {
                return false;
            }
        }
        return false;
    }

    @Override
    public String[] next() throws NoSuchElementException {
        if (this.lastLine != null) {
            String next = this.lastLine;
            this.lastLine = null;
            return next.split(",");
        }
        while (this.currentFile < files.length) {
            try {
                String line = reader.readLine();
                if (line != null) {
                    return line.split(",");
                } else {
                    nextFile();
                }
            } catch (IOException e) {
                LOG.error("Unable to read " + this.directory + "/" + files[this.currentFile] +
                        " Skipping to next file.", e);
            }
        }
        throw new NoSuchElementException("No more files. Total files read: " + this.currentFile);
    }

    /**
     * No-op. Not supported.
     */
    @Override
    public void remove() {
    }

    /**
     * Opens the next file according to currentFile.
     * @throws NoSuchElementException if all files are exhausted.
     */
    private void nextFile() throws NoSuchElementException {
        if (reader != null) {
            try {
                reader.close();
            } catch (IOException e) {
                LOG.warn("Unable to close file stream for " + this.directory + "/" + files[this.currentFile], e);
            }
        }
        this.currentFile ++;
        if(this.currentFile >= files.length) {
            throw new NoSuchElementException("No more files. Total files read: " + this.currentFile);
        }
        try {
            reader = new BufferedReader(new FileReader(this.directory + "/" + files[this.currentFile]));
        } catch (FileNotFoundException e) {
            LOG.error("Unable to open file " + this.directory + "/" + files[this.currentFile], e);
            nextFile();
        }
    }
}