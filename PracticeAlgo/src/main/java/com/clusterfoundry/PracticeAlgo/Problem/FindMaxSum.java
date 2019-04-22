package com.clusterfoundry.PracticeAlgo.Problem;

class FindMaxSum 
{ 
	/*Function to return max sum such that no two elements 
	are adjacent */
	int FindMaxSum(int arr[], int n) 
	{ 
		int incl = arr[0]; 
		int excl = 0; 
		int excl_new; 
		int i; 

		for (i = 1; i < n; i++) 
		{ 
			/* current max excluding i */
			excl_new = (incl > excl) ? incl : excl; 

			/* current max including i */
			incl = excl + arr[i]; 
			excl = excl_new; 
			System.out.println("excl_new: "+excl_new+" incl: "+incl+" excl: "+excl);
		} 

		/* return max of incl and excl */
		return ((incl > excl) ? incl : excl); 
	} 

	// Driver program to test above functions 
	public static void main(String[] args) 
	{ 
		FindMaxSum sum = new FindMaxSum(); 
		int arr[] = new int[]{2, 5, 10, 100, 10, 5}; 
		System.out.println(sum.FindMaxSum(arr, arr.length)); 
	} 
} 

// This code has been contributed by Mayank Jaiswal 

