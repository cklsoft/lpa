import org.apache.spark.graphx.Pregel._
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.storage.StorageLevel
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

import scala.reflect.ClassTag

object CCFLPA extends Logging with Serializable {
  def Pregel2[VD: ClassTag, ED: ClassTag, A: ClassTag]
  (g00:Graph[(Long,Long),Double],gg:Graph[(Long,Long),Double],graph: Graph[VD, ED],
   initialMsg: A,
   maxIterations: Int = Int.MaxValue,
   activeDirection: EdgeDirection = EdgeDirection.Either)
  (vprog: (VertexId, VD, A) => VD,
   sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
   mergeMsg: (A, A) => A)
  =
  {
    var g = graph.mapVertices((vid, vdata) => vprog(vid, vdata, initialMsg)).cache()//根据初始消息，生成节点的初始属性，这里原图的节点属性会变更
  // compute the messages
  var messages = g.mapReduceTriplets(sendMsg, mergeMsg)//以发送消息函数作为map，合并消息作为reduce。将由sendMsg生成的消息发给消息
  //对应的指定节点，节点利用mergeMsg合并收到的消息，返回<收到消息的节点id,该节点合并后的消息>
  var activeMessages = messages.count()//统计未收敛节点
    graph.unpersistVertices(blocking=false)
    graph.edges.unpersist(blocking=false)
    g00.unpersistVertices(blocking=false)
    g00.edges.unpersist(blocking=false)
    gg.unpersistVertices(blocking=false)
    gg.edges.unpersist(blocking=false)
    // Loop
    var prevG: Graph[VD, ED] = null
    var i = 0
    while (activeMessages > 0 && i < maxIterations) {
      // Receive the messages. Vertices that didn't get any messages do not appear in newVerts.
      val newVerts = g.vertices.innerJoin(messages)(vprog).cache()
      //对<收到消息的节点，收到的消息>与图中节点进行内连接，并调用vprog对同时出现在这两个节点集合的节点进行处理，
      // 返回更新的节点和其新属性
      // Update the graph with the new vertices.
      prevG = g
      g = g.outerJoinVertices(newVerts) { (vid, old, newOpt) => newOpt.getOrElse(old) }//将节点的新属性更新到原图中
      g.cache()//cache新图

      val oldMessages = messages
      // Send new messages. Vertices that didn't get any messages don't appear in newVerts, so don't
      // get to send messages. We must cache messages so it can be materialized on the next line,
      // allowing us to uncache the previous iteration.
      messages = g.mapReduceTriplets(sendMsg, mergeMsg, Some((newVerts, activeDirection))).cache()//更新新图：将由sendMsg生成的消息发给消息
      //对应的指定节点，节点利用mergeMsg合并收到的消息，返回<收到消息的节点id,该节点合并后的消息>
      // The call to count() materializes `messages`, `newVerts`, and the vertices of `g`. This
      // hides oldMessages (depended on by newVerts), newVerts (depended on by messages), and the
      // vertices of prevG (depended on by newVerts, oldMessages, and the vertices of g).
      activeMessages = messages.count()//统计未收敛节点数
      // Unpersist the RDDs hidden by newly-materialized RDDs
      oldMessages.unpersist(blocking=false)
      newVerts.unpersist(blocking=false)
      prevG.unpersistVertices(blocking=false)
      prevG.edges.unpersist(blocking=false)
      // count the iteration
      i += 1
    }

    g
  }
  def Pregel[VD: ClassTag, ED: ClassTag, A: ClassTag]
  (graph: Graph[VD, ED],
   initialMsg: A,
   maxIterations: Int = Int.MaxValue,
   activeDirection: EdgeDirection = EdgeDirection.Either)
  (vprog: (VertexId, VD, A) => VD,
   sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
   mergeMsg: (A, A) => A)
  : Graph[VD, ED] =
  {
    var g = graph.mapVertices((vid, vdata) => vprog(vid, vdata, initialMsg)).cache()//根据初始消息，生成节点的初始属性，这里原图的节点属性会变更
  // compute the messages
  var messages = g.mapReduceTriplets(sendMsg, mergeMsg)//以发送消息函数作为map，合并消息作为reduce。将由sendMsg生成的消息发给消息
  //对应的指定节点，节点利用mergeMsg合并收到的消息，返回<收到消息的节点id,该节点合并后的消息>
  var activeMessages = messages.count()//统计未收敛节点
    graph.unpersistVertices(blocking=false)
    graph.edges.unpersist(blocking=false)
    // Loop
    var prevG: Graph[VD, ED] = null
    var i = 0
    while (activeMessages > 0 && i < maxIterations) {
      // Receive the messages. Vertices that didn't get any messages do not appear in newVerts.
      val newVerts = g.vertices.innerJoin(messages)(vprog).cache()
      //对<收到消息的节点，收到的消息>与图中节点进行内连接，并调用vprog对同时出现在这两个节点集合的节点进行处理，
      // 返回更新的节点和其新属性
      // Update the graph with the new vertices.
      prevG = g
      g = g.outerJoinVertices(newVerts) { (vid, old, newOpt) => newOpt.getOrElse(old) }//将节点的新属性更新到原图中
      g.cache()//cache新图

      val oldMessages = messages
      // Send new messages. Vertices that didn't get any messages don't appear in newVerts, so don't
      // get to send messages. We must cache messages so it can be materialized on the next line,
      // allowing us to uncache the previous iteration.
      messages = g.mapReduceTriplets(sendMsg, mergeMsg, Some((newVerts, activeDirection))).cache()//更新新图：将由sendMsg生成的消息发给消息
      //对应的指定节点，节点利用mergeMsg合并收到的消息，返回<收到消息的节点id,该节点合并后的消息>
      // The call to count() materializes `messages`, `newVerts`, and the vertices of `g`. This
      // hides oldMessages (depended on by newVerts), newVerts (depended on by messages), and the
      // vertices of prevG (depended on by newVerts, oldMessages, and the vertices of g).
      activeMessages = messages.count()//统计未收敛节点数
      // Unpersist the RDDs hidden by newly-materialized RDDs
      oldMessages.unpersist(blocking=false)
      newVerts.unpersist(blocking=false)
      prevG.unpersistVertices(blocking=false)
      prevG.edges.unpersist(blocking=false)
      // count the iteration
      i += 1
    }

    g
  }
  /** *
    * 标签传播算法
    * @param g
    * @param maxIterNum
    * @return
    */
  def LPA(g00:Graph[(Long,Long),Double],g:Graph[(Long,Long),Double],maxIterNum:Int):Unit={
    val g2:Graph[Long,(Long,Long,Double)]=g.mapTriplets{e=>(e.srcAttr._2,e.dstAttr._2,e.attr)}//将边属性用于保存节点度，因为边属性整个过程是不更新的
      .mapVertices((id,attr)=>id)
    val initialMessage=Map[Long,(Double,Long,Long)]()
    def mergeMessage(c1:Map[Long,(Double,Long,Long)],c2:Map[Long,(Double,Long,Long)]):Map[Long,(Double,Long,Long)]={
      (c1.keySet++c2.keySet).map{k=>
        val v1=c1.getOrElse(k,(0.0,0L,0L))
        val v2=c2.getOrElse(k,(0.0,0L,0L))
        k->(v1._1+v2._1,v1._2+v2._2,v1._3+v2._3)
      }.toMap
    }
    def vprog0(id:VertexId,attr:Long,msg:Map[Long,(Double,Long,Long)]):Long={
      if(msg.isEmpty)attr
      else{//这边节点的标签是由邻居节点的标签所决定的，不包括自身标签，如果包括了，则在权值中无法计算。LPA算法就是节点的标签是由其上一次迭代的邻居的标签决定的
        msg.maxBy(
          x=>0.1*(x._2._1 / x._2._2)+0.9*x._2._3
        )._1
      }
    }
    def sendMessage(e:EdgeTriplet[Long,(Long,Long,Double)])={
      //这边只发送边e两个端点的信息到另一个端点
      Iterator((e.srcId,Map(e.dstAttr->(e.attr._3,1L,e.attr._2))),(e.dstId,Map(e.srcAttr->(e.attr._3,1L,e.attr._1))))
    }
    val lpag:Graph[Long,(Long,Long,Double)]=Pregel2(g00,g,g2, initialMessage, maxIterations = maxIterNum)(
      vprog = vprog0,
      sendMsg = sendMessage,
      mergeMsg = mergeMessage
    )
    lpag.vertices.map{
      case(id,v)=>
        id+" "+v
    }.saveAsTextFile("newLabels")
    lpag.unpersistVertices(blocking=false)
    lpag.edges.unpersist(blocking=false)
  }

  /**
   * 生成圈子，并根据圈子中节点的状态确定状态未知节点的状态
   * @param sc
   */
  def generateCircleAndEnsureStatus(sc:SparkContext):RDD[(VertexId,Int)] ={
    sc.textFile("lpain/school_user_sphere",5).map{s=>
      val h=s.split('|')
      if(h(2)=="u")
        (h(0).toLong,2)
      else (h(0).toLong,h(2).toInt)
    }.join(
        sc.textFile("newLabels",5).map{s=>
          val h=s.split(' ')
          (h(0).toLong,h(1).toLong)
        }
      ).map{
      case(k,(v1,v2))=>
        (v2,(k,v1))
    }.groupByKey(5).flatMap{//统计该圈子中学生和非学生的比例
      case(k,iter:Iterable[(Long,Int)])=>{
        var stuCnt=0
        val totCnt=iter.size
        var mp=ArrayBuffer[Long]()//保存圈子中所有未知的学生id
        var statuses=ArrayBuffer[(Long,Int)]()
        for(i<-iter){
          if(i._2==2)
            mp+=i._1
          else{
            if(i._2==1)
              stuCnt+=1
            statuses+=i
          }
        }
        if(stuCnt * 1.0 / totCnt - 0.01 >= 1e-7)//学生圈子
          stuCnt=3
        else stuCnt=2
        for (p<-mp)
          statuses+=((p,stuCnt))
        statuses
      }
    }
  }

  /**
   * 根据邻居节点状态，迭代更新状态未知的节点的状态
   * @param user
   * @param e
   * @param maxIterNum
   */
  def finalStatus(user:RDD[(VertexId,Int)],e:RDD[Edge[Double]],maxIterNum:Int): Unit ={
    val g:Graph[Int,Int]=Graph.fromEdges(e,(0L,0L),StorageLevel.MEMORY_AND_DISK,StorageLevel.MEMORY_AND_DISK).outerJoinVertices(user){
      (vid,old,newOpt)=>newOpt match{
        case Some(lab)=>lab
        case None=>0//如果没匹配到，就用自己的id作为属性
      }
    }.mapEdges(e=>0).cache()
    g.vertices.count()//使得可以删除原图和user
    user.unpersist(blocking=false)
    val initialMessage=(0L,0L)//学生权值和，非学生权值和
    def mergeMessage(c1:(Long,Long),c2:(Long,Long)):(Long,Long)={
      (c1._1+c2._1,c1._2+c2._2)
    }
    //照理说，应该只有一个状态未确定的节点会执行，状态确定的节点不会更新，即只有状态未确定的节点才会接收到消息
    def vprog0(id:VertexId,attr:Int,msg:(Long,Long)):Int={
      if(attr!=2) {
        logDebug("[CKL " + id.toString + "Received Msg" + attr.toString + "]")
        attr
      }
      else{
        var ans=attr
        if(ans==2&&msg._1>msg._2)//原本非学生，现在预测是学生
          ans=3
        else if(ans==3&&msg._2>msg._1)
          ans=2
        ans
      }
    }
    def sendMessage(e:EdgeTriplet[Int,Int])={//冗余？在第一次执行时是否有错？第一次执行每个节点都会调用到vprog
    var stuVal,noStuVal=0L
      if(e.srcAttr==0)noStuVal+=1
      else if(e.srcAttr==1)stuVal+=1
      //一个节点如果有执行该函数，说明该节点有接收到消息，说明该节点是未知节点，即需要给自己发一个消息
      if(e.dstAttr>1)
        Iterator((e.srcId,(stuVal,noStuVal)),(e.dstId,(stuVal,noStuVal)))
      else
        Iterator((e.srcId,(stuVal,noStuVal)))
    }
    val g2:Graph[Int,Int]=Pregel(g, initialMessage, maxIterations = maxIterNum)(
      vprog = vprog0,
      sendMsg = sendMessage,
      mergeMsg = mergeMessage
    )
    g.unpersistVertices(blocking=false)
    g.edges.unpersist(blocking=false)
    g2.vertices.map{
      case(vid:VertexId,status:Int)=>
        vid+" "+{if(status>1)status-2 else status}.toString
    }.saveAsTextFile("finalStatus")
    g2.unpersistVertices(blocking=false)//怎么办？
    g2.edges.unpersist(blocking=false)
  }

  /**
   * 计算节点边权
   * @param sc
   * @return
   */
  def createEdges(sc:SparkContext): RDD[Edge[Double]] ={
    val call=sc.textFile("lpain/school_call_sphere",5).map(s=>{
      s.split('|')//A B numCalls durtionCall
    }).filter{//过滤掉自己跟自己通信的数据
      s=>s(0)!=s(1)
    }.map{
      s=>{(s(0)+"_"+s(1),(s(2).toInt,s(3).toInt))}
    }.reduceByKey (
    {(a,b)=> (a._1 + b._1, a._2 + b._2)}
    ).map{
      case(id:String,(cnum,ctime))=>(id,ctime/2252.0/cnum)
    }
    val sms=sc.textFile("lpain/school_sms_sphere",5).map(s=>{
      s.split('|')//A B numCalls durtionCall
    }).filter{
      s=>s(0)!=s(1)
    }.map{
      s=>(s(0)+"_"+s(1),s(2).toInt)
    }.reduceByKey(
        _+_
      ).map{
      case(id,snum:Int)=>
        (id,snum/2252.0+snum/1220.0)
    }
    val edges:RDD[Edge[Double]]=call.union(sms).reduceByKey(
    {(a,b)=>a+b}
    ).map{
      case(a:String,b:Double)=>{
        val n=a.indexOf('_')
        val s1=a.substring(0,n).toLong
        val s2=a.substring(n+1,a.length).toLong
        Edge(s1,s2,b)
        //   Array(Edge(s1,s2,b),Edge(s2,s1,b))//这里双向边可以在Pregel中用消息进行控制,若采用结构上的双向边，这里要用flatMap
      }
    }
    edges
  }

  def main(args: Array[String]) {
    var sparkConf = new SparkConf().setAppName("CCFLPA")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "org.apache.spark.graphx.GraphKryoRegistrator")
    if(args.size>2){
      sparkConf=sparkConf.set("spark.default.parallelism",args(3))
      if(args.size>3)
        sparkConf=sparkConf.set("spark.storage.memoryFraction",args(4))
    }
    val sc=new SparkContext(sparkConf)
    val ckptdir="ckptdir/lpa"
    sc.setCheckpointDir(ckptdir)
    val e=sc.textFile(args(0),5).map(s=>s.split(' ')).filter(h=>h(0)!=h(1)).map(h=>Edge(h(0).toLong,h(1).toLong,h(2).toDouble)).cache()
    val defaultUsers=(0L,0L)
    val g0:Graph[(Long,Long),Double]=Graph.fromEdges(e,defaultUsers,StorageLevel.MEMORY_AND_DISK,StorageLevel.MEMORY_AND_DISK)
    val g=g0.outerJoinVertices(g0.degrees){
      (vid,old, newOpt)=>(vid,newOpt.getOrElse(0).toLong)//vertexAttr改成(label,deg)
    }
    LPA(g0,g,args(1).toInt)//标签传播
    //  label.saveAsTextFile("Labels")
    //不能释放掉g，因为g在finalStatus中用到
    val user=generateCircleAndEnsureStatus(sc)
    finalStatus(user,e,args(2).toInt)
  }
}