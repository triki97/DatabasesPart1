package com.chunkserver;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import com.interfaces.ChunkServerInterface;

/**
 * implementation of interfaces at the chunkserver side
 * 
 * @author Shahram Ghandeharizadeh
 *
 */

public class ChunkServer implements ChunkServerInterface {
	final static String filePath = "/Users/triki/Desktop/Spring2018/Databases/TinyFS-2"; // or C:\\newfile.txt
	public static long counter;
	final static String filePathForInitialization = "/Users/triki/Desktop/Spring2018/Databases/TinyFS-2/initializeFile.txt"; // or C:\\newfile.txt
	/**
	 * Initialize the chunk server
	 */
	public ChunkServer() {
		List<String> lines = Arrays.asList("0");
		Path initializationFile = Paths.get(filePathForInitialization); //Write the initialization file
		try {
			Files.write(initializationFile, lines, Charset.forName("UTF-8"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Each chunk corresponds to a file. Return the chunk handle of the last chunk
	 * in the file.
	 */
	public String initializeChunk() {
		String chunkHandle;
		StringBuilder sb = new StringBuilder();
		sb.append(filePath);
		sb.append("/");
		Scanner scanner = null;
		try {
			scanner = new Scanner(new File(filePathForInitialization));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		counter = scanner.nextLong();
		counter++;
		sb.append(counter);
		List<String> lines = Arrays.asList(Long.toString(counter));
		sb.append(".bin");
		chunkHandle = sb.toString();
		Path newChunkToWriteAsPath = Paths.get(chunkHandle); //Write the chunk file
		byte data[] = new byte[4096];
		try {
			Files.write(newChunkToWriteAsPath, data);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		Path initializationFile = Paths.get(filePathForInitialization); //Write the initialization file
		try {
			Files.write(initializationFile, lines, Charset.forName("UTF-8"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return chunkHandle;
	}

	/**
	 * Write the byte array to the chunk at the specified offset The byte array size
	 * should be no greater than 4KB
	 */
	public boolean putChunk(String ChunkHandle, byte[] payload, int offset) {
		if((4096-offset) > payload.length) {
			return false;
		}
	    Path p = FileSystems.getDefault().getPath("", ChunkHandle);
    	byte[] fileData;

	    try {
			fileData = Files.readAllBytes(p);
		} catch (IOException e) {
			return false;
		}
	    for(int i = offset; i < payload.length; i++) {
			fileData[i] = payload[i];
	    }
		Path newChunkToWriteAsPath = Paths.get(ChunkHandle); //Write the chunk file
		try {
			Files.write(newChunkToWriteAsPath, fileData);
		} catch (IOException e1) {
			return false;
		}
		return true;
	}

	/**
	 * read the chunk at the specific offset
	 */
	public byte[] getChunk(String ChunkHandle, int offset, int NumberOfBytes) {
    	byte[] dataToReturn = new byte[NumberOfBytes];
	    Path p = FileSystems.getDefault().getPath("", ChunkHandle);
    	byte[] fileData = null;
		try {
			fileData = Files.readAllBytes(p);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		for(int i = offset; i < NumberOfBytes; i++) {
			dataToReturn[i] = fileData[i];
		}
    	return dataToReturn;
	}

}
