#!/bin/usr/python
import sys
import csv
import copy
import os
from collections import OrderedDict

def errOr(num):
	if num  == 1:
		sys.exit("Syntex error Near from")
	if num == 2:
		sys.exit("Syntex error Near Select")
	if num == 3:
		sys.exit("Table not found")
	if num == 4:
		sys.exit("Sytex error Near aggregate function")
	if num == 5 :
		sys.exit("Please use aggregate function properly")
	if num == 6 :
		sys.exit("Unknown function")
	if num == 7 :
		sys.exit("Column not present in Table")
	if num == 8 :
		sys.exit("Use distinct Properly")
	if num == 9 :
		sys.exit("Either ambiguous column or column does not exist")
	if num == 10 :
		sys.exit("Either ambiguous column ")


def parseQuery(query,mydictionary):

	
	query = ' '.join(query.split())
	cntDistinct = query.count("distinct")
	if cntDistinct > 1:
		errOr(8)
	lis = []
	if "from" in query:
		splitQuery = query.split("from")
	else:
		errOr(1)

	splitQuery[0] = ' '.join(splitQuery[0].split())  
	
	temp = splitQuery[0].lower()
	
	if "select" in temp:
		temp = splitQuery[0][7:]
		lis.append("select")
	else:
		errOr(2)
	
	temp = ' '.join(temp.split())

	if "distinct" in temp:
		temp1 = temp
		temp1 = temp1.replace(" ", "")
		cntb = 0
		cntc = 0
		cntb = cntb+temp1.count("(")
		cntc = cntc+temp1.count(",")
		if cntb == 1 and cntc >=1:
			errOr(8)

		temp = temp.replace("(", "")
		temp = temp.replace(")", "")
		if temp[8] == '(' and temp[-1] ==')':
			disString = temp[temp.index("(")+1:temp.index(")")]
			disString = disString.strip()
			#print(disString)
			distList = disString.split(",")
			if(len(distList) > 1):
				errOr(8)
			else:
				temp = temp[9:temp.index(")")]

				lis.append("distinct")
				pass
		else:
			temp = temp[9:]
			lis.append("distinct")

	lis.append(temp)
	temp = lis
	
	distinctInQuery = ""
	if temp[1] == "distinct":
		distinctInQuery = temp[1]
		distinctInQuery =' '.join(distinctInQuery.split())
		disColum = temp[2] 	
	else:
		disColum = temp[1]

	disColum =' '.join(disColum.split())
	columName = disColum.split(",")
	for colu in columName:
		columName[columName.index(colu)] = ' '.join(colu.split())


	splitQuery[1] = ' '.join(splitQuery[1].split()) 
	splitQueryFW = splitQuery[1].split('where');

	tableNamesString = splitQueryFW[0]
	tableNamesString = ' '.join(tableNamesString.split())
	tableNameFQ = tableNamesString.strip().split(",")
	
	for q in tableNameFQ:
		tableNameFQ[tableNameFQ.index(q)] = q.strip()
	

	for q in tableNameFQ:
		if q not in mydictionary.keys():
			errOr(3)
	noOfTable = len(tableNameFQ)
	noOfColum = len(columName)
	if noOfTable == 1 and len(splitQueryFW) > 1:
		splitQueryFW[1] = ' '.join(splitQueryFW[1].split())
		if(columName[0] == '*' and len(columName) == 1):
			columName = mydictionary[tableNameFQ[0]]
		fileContent = readFiledata(tableNameFQ)	
		processNormalWhere(splitQueryFW[1],columName,tableNameFQ,fileContent,mydictionary)
		return 
	
	elif noOfTable > 1 and len(splitQueryFW) > 1:
		processWhereWithJoin(splitQueryFW[1],columName,tableNameFQ,mydictionary)
		return

	if(noOfTable > 1):
		join(columName,tableNameFQ,mydictionary)
		return

	if distinctInQuery == "distinct":
		tableNameTP = tableNameFQ[0] + '.csv'
		fileContent = []
		readCsv(tableNameTP,fileContent)
		distinctQuery(columName,tableNameFQ,mydictionary,fileContent)
		return 
	
	if(noOfColum == 1):
		if '(' in columName[0] and ")" in columName[0]:
			fun = ""
			colOnWhichaggre = ""
			start = columName[0].index("(")
			end = columName[0].index(")")
			fun = columName[0][:start]
			colOnWhichaggre = columName[0][start+1:end]
			if colOnWhichaggre == '*':
				errOr(5)
			if "." in colOnWhichaggre:
				colOnWhichaggre = colOnWhichaggre[colOnWhichaggre.index(".")+1:]
			if colOnWhichaggre not in mydictionary[tableNameFQ[0]]:
				errOr (5)
			fun = fun.lower()
			aggreGate(fun,colOnWhichaggre,tableNameFQ,mydictionary)
			return
		elif '(' in columName[0] or ")" in columName[0]:
			errOr(4)

	noOfColum = len(columName)
	if(columName[0] == '*' and noOfColum == 1):
		columName = mydictionary[tableNameFQ[0]]
	printAllTable(columName,tableNameFQ,mydictionary)

############ complete it later ############
def distinctQuery(columName,tableNameFQ,mydictionary,fileContent):

	headerPrint(columName,tableNameFQ,mydictionary)
	tempData=[]
	check = True
	for line in fileContent:
		for column in columName:
			check = False
			data = line[mydictionary[tableNameFQ[0]].index(column)]
			if data not in tempData:
				check = True
				tempData.insert(0,data)
				print(data)+" ",
		print
#############################################
def join(columName,tableNameFQ,mydictionary):
	tableOrig = tableNameFQ[:]
	temdir = mydictionary.copy()
	columName,tableNameFQ,mydictionary,joinedData = Joining(columName,tableNameFQ,mydictionary)
	cnt = 1
	check = False
	if len(columName) == 1:
		check = True
		if "." in columName[0]:
			check = False
		else:
			if columName[0]    in temdir[tableOrig[0]]:
				cnt = cnt+1
			
			if columName[0] in temdir[tableOrig[1]]:
		 		cnt = cnt+1
	if check:
		if cnt == 1 or cnt == 3:
			errOr(9)
	for colum in columName:
		check = True
		print colum,
	print
	check = True
	for line in joinedData:
		for col in columName:
			check = False
			if '.' in col:
				print(line[mydictionary[tableNameFQ[0]].index(col)]),
			else:
				check = True
				print line[mydictionary["normalAdd"].index(col)],
		print

########################################################
def Joining(columName,tableNameFQ,mydictionary):
	table1Data = []
	table2Data = []
	tableName1 = tableNameFQ[0] + '.csv'
	tableName2 = tableNameFQ[1] + '.csv'
	readCsv(tableName1,table1Data)
	readCsv(tableName2,table2Data)
	mydictionary["naturalJoin"]=[]
	joinedData = []
	for i in range(2):
		for coluName in mydictionary[tableNameFQ[i]]:
			mydictionary["naturalJoin"].append(tableNameFQ[i]+"."+coluName)
	for data2 in  table2Data:
		for data1 in table1Data:
			joinedData.append(data1+data2)
	mydictionary["normalAdd"] = mydictionary[tableNameFQ[0]]+mydictionary[tableNameFQ[1]]
	if(columName[0]=="*" and len(columName) ==1):
		columName = mydictionary["naturalJoin"]
	tableNameFQ.remove(tableNameFQ[0])
	tableNameFQ.remove(tableNameFQ[0])
	tableNameFQ.insert(0,"naturalJoin")
	return columName,tableNameFQ,mydictionary,joinedData


############################################################
def processWhereWithJoin(stringaWhere,columName,tableNameFQ,mydictionary):
	
	stringaWhere = spaceMaker(stringaWhere)
	stringaWhere = ' '.join(stringaWhere.split())
	strWhere = stringaWhere.split(" ")
	# print(tableNameFQ)
	strMode = []
	cnt = 0 
	for i in strWhere:
		if "." in i or i == "=" or i == ">" or i == "<" or i == "and" or i=="or"or i[0].isdigit():
			strMode.append(i)
		else:
			if cnt == 0:
				strMode.append(tableNameFQ[cnt]+"."+i)
				cnt = 1
			else:
				strMode.append(tableNameFQ[cnt]+"."+i)

	columName,tableNameFQ,mydictionary,joinedData = Joining(columName,tableNameFQ,mydictionary)
	
	for colum in columName:
		print colum,
	print
	
	check = False
	for line in joinedData:
		val = checkAndOr(strMode,line,tableNameFQ,mydictionary)  ##changed from strWhere-->strMode
		for col in columName:
			if eval(val):
				check = True
				if '.' in col:
					print(line[mydictionary[tableNameFQ[0]].index(col)]),
					check1 = True
				else:
					print line[mydictionary["normalAdd"].index(col)],
		if check:
			check = False
			print

#################################
def aggreGate(fun,colOnWhichaggre,tableNameFQ,mydictionary):
	#print("aggreGate")
	index = mydictionary[tableNameFQ[0]].index(colOnWhichaggre)
	ansList = []
	fileContent = []
	tableNameTP = tableNameFQ[0] + '.csv'
	readCsv(tableNameTP,fileContent)
	for line in fileContent:
		ansList.append(int(line[index]))
	if fun == "sum":
		print(sum(ansList))
	elif fun == "average":
		print(sum(ansList)/len(ansList))
	elif fun == "max":
		print(max(ansList))
	elif fun == "min":
		print(min(ansList))
	else:
		errOr(6)

def printAllTable(columName,tableNameFQ,mydictionary):

	for cn in columName:
		if cn not in mydictionary[tableNameFQ[0]]:
			errOr(7)
	headerPrint(columName,tableNameFQ,mydictionary)
	fileContent = readFiledata(tableNameFQ)
	printTableData(fileContent,columName,tableNameFQ,mydictionary)

def headerPrint(columName,tableNameFQ,mydictionary):
	
	print" OUTPUT : " 
	headerstring = ""
	for colmn in columName:
		for tble in tableNameFQ:
			check = True
			if colmn in mydictionary[tble]:
				if not headerstring == "":
					check = True
					headerstring = headerstring +','
					check = False
				headerstring = headerstring + tble +'.'+colmn
	print(headerstring)

def readFiledata(tableNameFQ):
	tableNameTP = tableNameFQ[0] + '.csv'
	fileContent = []
	readCsv(tableNameTP,fileContent)
	return fileContent

def readCsv(tableNameTP,fileContent):
	if os.access(tableNameTP,os.R_OK):
		with open(tableNameTP, 'rb') as table:
			fileData = csv.reader(table)
			for line in fileData:
				fileContent.append(line)
	else:
		sys.exit("can't access database")
def spaceMaker(stringaWhere):
	if "=" in stringaWhere:
		cnt = stringaWhere.count("=")
		if cnt == 1:
			stringaWhere = stringaWhere.split("=")
			stringaWhere = stringaWhere[0]+" "+"="+" "+stringaWhere[1]
		if cnt == 2:
			stringaWhere = stringaWhere.split("=")
			stringaWhere = stringaWhere[0]+" "+"="+" "+stringaWhere[1]+" "+"="+" "+stringaWhere[2]
	if "<" in stringaWhere:
		cnt = stringaWhere.count("<")
		if cnt == 1:
			stringaWhere = stringaWhere.split("<")
			stringaWhere = stringaWhere[0]+" "+"<"+" "+stringaWhere[1]
		if cnt == 2:
			stringaWhere = stringaWhere.split("<")
			stringaWhere = stringaWhere[0]+" "+"<"+" "+stringaWhere[1]+" "+"<"+" "+stringaWhere[2]
	if ">" in stringaWhere:
		cnt = stringaWhere.count(">")
		if cnt == 1:
			stringaWhere = stringaWhere.split(">")
			stringaWhere = stringaWhere[0]+" "+">"+" "+stringaWhere[1]
		if cnt == 2:
			stringaWhere = stringaWhere.split(">")
			stringaWhere = stringaWhere[0]+" "+">"+" "+stringaWhere[1]+" "+">"+" "+stringaWhere[2]	
	stringaWhere = ' '.join(stringaWhere.split())
	return stringaWhere
	
def processNormalWhere(stringaWhere,columName,tableNameFQ,fileContent,mydictionary):
	
	stringaWhere = spaceMaker(stringaWhere)
	#print("----------------")
	stringaWhere = ' '.join(stringaWhere.split())
	lis = []
	columUpdate = []
	for cn in columName:
		if "." in cn:
			lis.append(cn[:cn.index(".")])
			columUpdate.append(cn[cn.index(".")+1:])
		else:
			columUpdate.append(cn)
	
	for tn in lis:
		if tn <> tableNameFQ[0]:
			errOr(7)

	for cn in columUpdate:
		if cn not in mydictionary[tableNameFQ[0]]:
			errOr(7)
	headerPrint(columName,tableNameFQ,mydictionary)
	stringaWhere = ' '.join(stringaWhere.split())
	lisT=[]
	lisC =[]
	strWwdot=[]
	strWhere = stringaWhere.split(" ")
	for i in strWhere:
		if "." in i:
			strWwdot.append(i[i.index(".")+1:])
			lisT.append(i[:i.index(".")])
			lisC.append(i[i.index(".")+1:])
		else:
			strWwdot.append(i)
			if i <> ">" and i <> "<" and i <> "=" and i <> "and" and i<> "or" and not (i[0].isdigit()):
				lisC.append(i)

	for tn in lisT:
	 	if tn <> tableNameFQ[0]:
			errOr(7)

	for cn in lisC:
		if cn not in mydictionary[tableNameFQ[0]]:
	 		errOr(7)


	check = False
	for line in fileContent:
		val = checkAndOr(strWwdot,line,tableNameFQ,mydictionary)
		for col in columUpdate:
			if eval(val):
				check = True
				print line[mydictionary[tableNameFQ[0]].index(col)],
		if check:
			check = False
			print

def checkAndOr(strWhere,line,tableNameFQ,mydictionary):
	val = ""
	for i in strWhere:
		if i in mydictionary[tableNameFQ[0]]:
			val = val + line[mydictionary[tableNameFQ[0]].index(i)]
		elif i.lower() == "or" or i.lower() == "and":
			val  = val +" "+i.lower()+" "
		elif i == "=":
			val = val + "=="
		else:
			val = val+i
	return val

def printTableData(fileContent,columName,tableNameFQ,mydictionary):
	check = True 
	for row in fileContent:
		for colu in columName:
			check = False
			print row[mydictionary[tableNameFQ[0]].index(colu)],
		print

def readMetadata(mydictionary):
	file = open('./metadata.txt','r')
	name = False
	for line in file:
		if line.strip() == "<begin_table>":
			name = True # if Next line is table name 
			continue
		if name :
			tableName = line.strip()
			mydictionary[tableName] = [];
			name = False
			continue
		if line.strip() <> '<end_table>': # add attribulte to the dictionary
			mydictionary[tableName].append(line.strip());		


def main():
	mydictionary = {}
	readMetadata(mydictionary)
	#print(mydictionary)
	parseQuery(str(sys.argv[1]),mydictionary)


if __name__ == '__main__':
	main()
