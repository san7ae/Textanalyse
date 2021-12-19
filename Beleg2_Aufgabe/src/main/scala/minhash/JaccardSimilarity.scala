package minhash

import scala.util.Random

object JaccardSimilarity {
  
  val randgen= new Random
  
  /*
   * 
   * Calculate the Jaccard Distance of two Sets
   * 
   */
  def calculateJaccardDistanceSet[T](set1:Set[T], set2:Set[T]):Double= {
    val intersect = set1.intersect(set2)
    val union = set1.union(set2)
    intersect.size.toDouble/union.size.toDouble
  }
  

   /*
 	 * 
   * Calculate the Jaccard Distance of two Bags
   * 
   */

  def calculateJaccardDistanceBag[T](bag1:Iterable[T], bag2:Iterable[T]):Double= {
    val intersect = bag1.filter(x => bag2.toSet.contains(x))
    val union = bag1 ++ bag2
    intersect.size.toDouble/union.size.toDouble
  }

  /*
 *
 * Calculate an Array of Hash Functions
 * The size of the array should be nrHashFuns
 *
 * Each function of the array should have the following structure
 * h(x)= m*x + b mod c, where
 *
 *    m is a random integer (between 0 and prim)
 *    b is a random integer (between 0 and prim)
 *    c is the parameter prim, that is passed in the signature of the method
 *
 * (The parameter prim should be the next prime number that is greater than nrHashFuns)
 *
 * --added on 18.12 : (m * x + b) % prim
 */

  def createHashFuntions(prim:Integer, nrHashFuns: Int):Array[(Int=>Int)]= {
    val m = if(prim > nrHashFuns) Random.nextInt(prim) else throw new Exception("Number of Hashfunction must be greater than prime number")
    val b = Random.nextInt(prim)
    (for (i <- 0 until nrHashFuns) yield (x : Int) => ((m*x) + b)%prim).toArray
  }

  /*
   * Implement the MinHash algorithm presented in the lecture
   * 
   * Input:
   * matrix: Document vectors (each column should corresponds to one document)
   * hFuns: Array of Hash-Functions
   * 
   * Output:
   * Signature Matrix:
   * columns: Each column corresponds to one document
   * rows: Each row corresponds to one hash function
   */
  
  def minHash[T](matrix:Array[Array[T]],hFuns:Array[Int=>Int]):Array[Array[Int]]= ???  
  
  /*++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
   * 
   * Helper functions that are used in the tests
   * 
   * +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
   */
  
  def printMultipleSets(data:Array[Array[Int]]):Unit= {
    data.foreach(x=>println(x.mkString(" ")))
  }
  
  def createRandomSetAsArray(nrElements:Int):Array[Int]= {
    val res= Array.tabulate(nrElements)(_=>0)
    (for (i <- 0 to nrElements-1) {
      
      if (randgen.nextFloat<0.3) res(randgen.nextInt(nrElements-1))=1
    })
    res
  }
  
  def transformArrayIntoSet(data:Array[Int]):Set[Int]={
    
    (for (i <- 0 to data.size-1 if (data(i)==1)) yield i).toSet 
   
  }
  
  def findNextPrim(x:Int):Int={

    def isPrim(X:Int, i:Int, Max:Int):Boolean = {
       if (i>=Max) true
          else if (X % i == 0) false
            else isPrim(X,i+1,Max)
    }
          
    if (isPrim(x,2,math.sqrt(x).toInt+1)) x 
      else findNextPrim(x+1)
  }
  
   def compareSignatures(sig1: Array[Int], sig2:Array[Int]):Int={
     
     var res=0
     for (i <- 0 to sig1.size-1) {if (sig1(i)==sig2(i)) res=res+1}
     res
   }
    
}