// Package crc 提供存储层统一的校验和实现：crc32c（Castagnoli）+ LevelDB 的掩码变形。
//
// 为什么单独抽一个包：WAL 的记录校验与 SSTable 的块校验**必须是同一套算法**。
// 两份独立实现只要有一点差别（多项式不同、掩码算错、字节序不一致），
// 就会出现"写的时候能过、读的时候报损坏"这类极难定位的问题，所以集中放在这里。
//
// 掩码（mask）的作用：裸 CRC 在"负载以 0 开头且长度字段被翻转"这类损坏下
// 可能恰好算回同一个值，掩码通过旋转再加常量把这种情况的碰撞概率压下去。
package crc

import "hash/crc32"

// Table 是 crc32c 的查表实现，等价于 LevelDB 的 crc32c::GetCrc32Table()。
var Table = crc32.MakeTable(crc32.Castagnoli)

// maskDelta 与 LevelDB 取值一致。
const maskDelta = 0xa282ead8

// Mask 把原始 CRC 变换成落盘使用的形态。
func Mask(crc uint32) uint32 {
	return ((crc >> 15) | (crc << 17)) + maskDelta
}

// Unmask 是 Mask 的逆运算，主要用于诊断"到底哪一段字节变了"。
func Unmask(masked uint32) uint32 {
	rot := masked - maskDelta
	return (rot >> 17) | (rot << 15)
}

// Checksum 返回 b 的掩码校验和。
func Checksum(b []byte) uint32 {
	return Mask(crc32.Checksum(b, Table))
}

// Update 在已有原始 CRC 的基础上继续累加，返回**未掩码**的原始值。
//
// 需要分段计算（例如"内容 + 类型字节"）时用它拼出原始值，最后再 Mask 一次。
func Update(crc uint32, b []byte) uint32 {
	return crc32.Update(crc, Table, b)
}

// ChecksumWithType 计算"内容 + 1 字节类型"的掩码校验和。
//
// SSTable 的每个块尾部都是"压缩类型 1 字节 + CRC 4 字节"，且校验范围要覆盖
// 类型字节本身（否则类型被翻转不会被发现），这个组合因此固定下来。
func ChecksumWithType(b []byte, t byte) uint32 {
	var one [1]byte
	one[0] = t
	return Mask(Update(Update(0, b), one[:]))
}
