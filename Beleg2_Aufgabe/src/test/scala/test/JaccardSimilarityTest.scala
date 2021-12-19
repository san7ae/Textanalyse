package test

import org.scalatest.funsuite.AnyFunSuite
import minhash._

class JaccardSimilarityTest  extends AnyFunSuite {
  

    trait teststrings{
   
     val bag1=List("this","is", "a", "test","this","is", "a", "test","hello", "world") 
     val bag2=List("this","is", "a", "test","this","is", "a", "test","cat","dog") 
     val funs:Array[Int=>Int]= Array(((x:Int)=>(x+1)%5),((x:Int)=>(3*x+1)%5))
     val matrix= Array(Array(1,0,0,1),Array(0,0,1,0),Array(0,1,0,1),Array(1,0,1,1),
         Array(0,0,1,0))
  }
  
  test("Jaccquard Similarity Set"){
    
    new teststrings{
      
      val res= JaccardSimilarity.calculateJaccardDistanceSet(bag1.toSet, bag2.toSet)
      assert(res===0.5)
    }
  }
  
  test("Jaccquard Similarity Bag"){
    
    new teststrings{
      
      val res= JaccardSimilarity.calculateJaccardDistanceBag(bag1, bag2)
      assert(res===0.4)
    }
  }
  
  test("Calculate Random Array"){
          
      val res= JaccardSimilarity.createRandomSetAsArray(15)
      println("Random Set in Array Representation")
      println(res.mkString(" "))
      println("Random Set in Set Representation")
      println(JaccardSimilarity.transformArrayIntoSet(res).mkString(" "))
  }
  
  test("Compare Signatures"){
      
    
      val sig1= Array(2,1,1,0,1,1,0,6,1,4,0,1)
      val sig2= Array(3,0,1,1,0,1,1,0,1,0,1,1)
      val res=JaccardSimilarity.compareSignatures(sig1, sig2)
      assert(res==4)
  }
  
  test("Minhashing Example Lecture"){

    new teststrings{
      
      val res= JaccardSimilarity.minHash(matrix,funs)
      assert(res===Array(Array(1,3,0,1), Array(0,2,0,0)))
    }

  }
  
  test("Minhashing Bigger Set"){

    val size= 10000
    val set1= JaccardSimilarity.createRandomSetAsArray(size)
    val set2= JaccardSimilarity.createRandomSetAsArray(size)
    val set1A= JaccardSimilarity.transformArrayIntoSet(set1)
    val set2A= JaccardSimilarity.transformArrayIntoSet(set2)
    val all= set1A union set2A
    val data= Array(set1,set2).transpose
    val equalSets= JaccardSimilarity.calculateJaccardDistanceSet(set1A, set1A)
    assert(equalSets===1.0)
    val jS= JaccardSimilarity.calculateJaccardDistanceSet(set1A,set2A)
    println("Jaccard-Distance is: "+jS)    
    val funs=JaccardSimilarity.createHashFuntions(
        JaccardSimilarity.findNextPrim(size), 400)
    val res= JaccardSimilarity.minHash(data,funs).transpose
    val sim= JaccardSimilarity.compareSignatures(res(0), res(1)).toDouble/funs.size
    println("MinHash-Jaccard-Distance is: "+sim)
    
    assert(math.abs(jS-sim)<0.05,"The difference between the two distances should be smaller than 0.05 - can fail occasionally")

  }
  
  test("Find next Prim Test"){
    
    assert(13==JaccardSimilarity.findNextPrim(12))
    assert(13==JaccardSimilarity.findNextPrim(13))
    assert(11==JaccardSimilarity.findNextPrim(8))
    assert(19==JaccardSimilarity.findNextPrim(JaccardSimilarity.findNextPrim(14)+1))
  }
}

