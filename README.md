# mjson

mjson 用于对于多个输入序列在一定的周期中进行合并输出合并的结果．对于不同序列需要生成不同的MergeManager用于管理数据的聚合．
实现上主要依赖于reflect包完成对于结构体的分析和数据操作

当前支持的聚合操作：

| 类型名称 | 含义  |
|---|---|
|  float64_avg | float64的平均值  |
|  float64_plus | float64取和  |
|  int_avg | int类型平均值  |
|  append_str | string slice数据追加  |
|   keep | 保持第一个值  |
|  replace | 保持最后一个值  |
|  string_concat | string拼接  |
|  []struct | 数组类型聚合  |


实例：

聚合１秒内接收到的所有Person信息, Person的聚合规则通过tag的形式保存．
比如
- name直接进行拼接
- age年龄进行取平均值
- location进行slice的append操作
- Salary进行求和操作

```go
type Person struct {
	Name      string   `json:"name" merge:"string_concat"`
	Age       int      `json:"age" merge:"int_avg"`
	Locations []string `json:"locations" merge:"append_str"`
	Salary    int      `json:"salary" merge:"int_plus"`
}

manager, err  := mjson.NewMergeManager(time.Duration(1) * time.Second, reflect.TypeOf(DNSInfo{}))
if err != nil{
    panic(err)
}

```

数据通过manager.Push函数推送即可．数据会定期的输出到manager.Output通道中．

```go
d1 = &Person{．．．}
d2 = &Person{．．．}
d3 = &Person{．．．}

manager.Push(d1)
manager.Push(d2)
manager.Push(d3)

data := <-manager.Output
fmt.Printf("%+v", data)
```