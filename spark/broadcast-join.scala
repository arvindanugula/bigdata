case class Item(id:String, name:String, unit:Int, companyId:String)
case class Company(companyId:String, name:String, city:String)


 val i1 = Item("1", "first", 1, "c1")
    val i2 = Item("2", "second", 2, "c2")
    val i3 = Item("3", "third", 3, "c3")
    val c1 = Company("c1", "company-1", "city-1")
    val c2 = Company("c2", "company-2", "city-2")

    val companies = sc.parallelize(List(c1,c2))
    val items = sc.parallelize(List(i1,i2,i3))
 
    val companiesDF = companies.toDF();
    val itemsDF = items.toDF();
 
    val itemsDFAlias = itemsDF.as("itemsDFAs");
    val companiesDFAlias = companiesDF.as("companiesDFAs")

	import org.apache.spark.sql.functions.broadcast
	
    val joinedDF =  itemsDFAlias.join(broadcast(companiesDFAlias), col("itemsDFAs.companyId") === col("companiesDFAs.companyId"),"inner")
	
    val joinedDF = itemsDFAlias.join(companiesDFAlias, col("itemsDFAs.companyId") === col("companiesDFAs.companyId"),"inner")
	
	
	itemsDFAlias.join(broadcast(companiesDFAlias), col("itemsDFAs.companyId") === col("companiesDFAs.companyId"),"inner").show()
	itemsDFAlias.join(companiesDFAlias, col("itemsDFAs.companyId") === col("companiesDFAs.companyId"),"inner").show()

    



