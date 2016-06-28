package com.doslocos.nutch.util;


import java.util.Arrays;


public class BBWrapper {

	
	private final byte[] bytes;
	
	public BBWrapper( byte[] bytes ) {
		if( null == bytes ) bytes = new byte[0];
		this.bytes = bytes;
	}
	
	@Override
    public boolean equals( Object rhs ) {
		return rhs instanceof BBWrapper && Arrays.equals( bytes, ( (BBWrapper)rhs ).bytes );
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode( bytes );
    }

    @Override
    public String toString() {
    	return new String( bytes );
    }
    
    
    public byte[] getBytes() {
    	return bytes;
    }
    
    public byte[] concat( final BBWrapper rhs ) {
    	if( null == rhs ) {
    		return bytes;
    	}
    	
    	byte[] result = new byte[ bytes.length + rhs.bytes.length ];
		System.arraycopy( bytes, 0, result, 0, bytes.length);
		System.arraycopy( rhs.bytes, 0, result, bytes.length, rhs.bytes.length);
		
		return result;
    }
    
    
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
