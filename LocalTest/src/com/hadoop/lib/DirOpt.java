package com.hadoop.lib;

import java.io.File;

public class DirOpt {
	
	public static void deleteDirectory(String filePath){
		
		File file = new File(filePath);
		
		if(file.exists()){
			if(file.isFile()){
				System.out.println("Cleaning File " + file.toString());
				file.delete();//清理文件
			}else{
				File[] list = file.listFiles();
				if(list!=null){
					for(File f : list){
						deleteDirectory(f.toString());
					}
					System.out.println("Cleaning Dir " + file.toString());
					file.delete();//清理目录
				}
			}
		}
	}
	
	public static void deleteDirectory(String filePath, Boolean showMsgFlag){
		
		File file = new File(filePath);
		
		if(file.exists()){
			if(file.isFile()){
				if(showMsgFlag) {
					System.out.println("Cleaning File " + file.toString());
				}
				file.delete();//清理文件
			}else{
				File[] list = file.listFiles();
				if(list!=null){
					for(File f : list){
						deleteDirectory(f.toString(), showMsgFlag);
					}
					if(showMsgFlag) {
						System.out.println("Cleaning Dir " + file.toString());
					}
					file.delete();//清理目录
				}
			}
		}
	}
}
