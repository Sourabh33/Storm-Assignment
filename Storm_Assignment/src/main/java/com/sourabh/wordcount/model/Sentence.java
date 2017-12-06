package com.sourabh.wordcount.model;

import java.io.Serializable;

public class Sentence implements Serializable{

	
	private static final long serialVersionUID = -5108281307856427934L;

	private String[] sentences;
	
	public Sentence() {
		sentences = new String[] {
						"A day of good morning",
						"A day of coffee",
						"A day with new Dawn",
						"A life full of Light"};
	}

	public String[] getSentences() {
		return sentences;
	}
	
	
}
