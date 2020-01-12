#!/usr/bin/env python
# -*- coding:utf-8 -*-
#
"""
封装 ogr 相关操作

Authors: changhuan(changhuan@baidu.com)
Date:    2019/6/10
"""
import os
from osgeo import ogr


def make_geometry_from_wkt(wkt):
    """
    通过wkt构造geometry
    :param wkt:
    :return:
    """
    return ogr.CreateGeometryFromWkt(wkt)


def export_to_wkt(geometry):
    """
    将geometry转换为wkt
    :param geometry:
    :return:
    """
    return geometry.ExportToWkt()


def make_point(x, y):
    """
    构造一个 point
    :param x:
    :param y:
    :return:
    """
    p = ogr.Geometry(ogr.wkbPoint)
    p.AddPoint(x, y)
    return p


def make_3dpoint(x, y, z):
    """
    构造一个三维的点
    :param x:
    :param y:
    :param z:
    :return:
    """
    p = ogr.Geometry(ogr.wkbPoint25D)
    p.AddPoint(x, y, z)
    return p


def get_mid_of_tow_point(p1, p2):
    """
    构造两个点的中点
    :param p1:
    :param p2:
    :return:
    """
    x = (p1.GetX() + p2.GetX()) / 2
    y = (p1.GetY() + p2.GetY()) / 2
    return make_point(x, y)


def points_equals(p1, p2):
    """
    比较p1和p2是否同一个点
    :param p1:
    :param p2:
    :return:
    """
    return p1.GetX() == p2.GetX() and p1.GetY() == p2.GetY()


def get_outer_ring(polygon):
    """
    获得四边形外边的环
    :param polygon:
    :return:
    """
    return polygon.GetGeometryRef(0)


def get_point_count(line):
    """
    获取点的数量
    :param line:
    :return:
    """
    return line.GetPointCount()


def get_point_from_line(line, index):
    """
    获得线段中的第index个点
    :param line
    :param index:
    :return:
    """
    p_xy = line.GetPoint(index)
    return make_point(p_xy[0], p_xy[1])


def get_3dpoint_from_line(line, index):
    """
    获得线段中的第index个点
    :param line
    :param index:
    :return:
    """
    p_xy = line.GetPoint(index)
    return make_3dpoint(p_xy[0], p_xy[1], p_xy[2])


def set_point_in_line(line, index, point):
    """
    设置线段中的第index个点
    :param line:
    :param index:
    :param point:
    :return:
    """
    line.SetPoint(index, point.GetX(), point.GetY(), point.GetZ())


def make_buffer(polygon, distance):
    """
    构造buffer
    :param polygon:
    :param distance:
    :return:
    """
    return polygon.Buffer(distance)


def get_centroid_of_geometry(geometry):
    """
    获得图形的中心点
    :param geometry:
    :return:
    """
    return geometry.Centroid()


def overlaps(geometry1, geometry2):
    """
    判断两个图形是否重叠
    :param geometry1:
    :param geometry2:
    :return:
    """
    return geometry1.Overlaps(geometry2)


def contains(geometry1, geometry2):
    """
    判断图形1 是否包含图形2
    :param geometry1:
    :param geometry2:
    :return:
    """
    return geometry1.Contains(geometry2)


def touches(geometry1, geometry2):
    """
    判断图形1 和图形2 是否接触
    :param geometry1:
    :param geometry2:
    :return:
    """
    return geometry1.Touches(geometry2)


def crosses(geometry1, geometry2):
    """
    判断图形1 和图形 2 是否交叉
    :param geometry1:
    :param geometry2:
    :return:
    """
    return geometry1.Crosses(geometry2)


def two_lines_coincident(line1, line2):
    """
    判断两个 line 是否重合
    :param line1:
    :param line2:
    :return:
    """
    count1 = get_point_count(line1)
    count2 = get_point_count(line2)
    if count1 != count2:
        return False
    for i in range(count1):
        if not points_equals(get_point_from_line(line1, i), get_point_from_line(line2, i)):
            return False
    return True


def get_intersection(geometry1, geometry2):
    """
    获得两个图形的交集,
    特别的，线段与多边形的交集是多个线段
    :param geometry1:
    :param geometry2:
    :return:
    """
    return geometry1.Intersection(geometry2)


def get_union(geometry1, geometry2):
    """
    获得两个图形的并集
    :param geometry1:
    :param geometry2:
    :return:
    """
    return geometry1.Union(geometry2)


def get_distance(geometry1, geometry2):
    """
    获得两个图形之间的距离
    :param geometry1:
    :param geometry2:
    :return:
    """
    return geometry1.Distance(geometry2)


def get_line_length(line):
    """
    获得line 的长度
    :param line:
    :return:
    """
    return line.Length()


def is_points_in_clockwise(p1, p2, p3):
    """
    判断三个点是否是顺时针
    :param p1:
    :param p2:
    :param p3:
    :return:
    """
    fs = (p2.GetX() - p1.GetX(), p2.GetY() - p1.GetY())
    ft = (p3.GetX() - p1.GetX(), p3.GetY() - p1.GetY())
    x = fs[0] * ft[1] - fs[1] * ft[0]
    if x < 0:
        return True
    elif x > 0:
        return False


def is_points_in_anti_clockwise(p1, p2, p3):
    """
    判断三个点是否是逆时针
    :param p1:
    :param p2:
    :param p3:
    :return:
    """
    fs = (p2.GetX() - p1.GetX(), p2.GetY() - p1.GetY())
    ft = (p3.GetX() - p1.GetX(), p3.GetY() - p1.GetY())
    x = fs[0] * ft[1] - fs[1] * ft[0]
    if x > 0:
        return True
    elif x < 0:
        return False

def check_is_clockwise(linestring):
	"""
	判断复杂多边形（如凹多边形）是否是顺时针，通过判断第一个符合要求的三个点的夹角确定
	:param linestring:
	:return:
	"""
	polygon = ogr.Geometry(ogr.wkbPolygon)
	polygon.AddGeometry(linestring)
	point_count = linestring.GetPointCount()
	if point_count < 3:
		return False
	for i in range(point_count - 2):
		p1 = linestring.GetPoint(i)
		p2 = linestring.GetPoint(i + 1)
		p3 = linestring.GetPoint(i + 2)
		x = (p1[0] + p2[0] + p3[0]) / 3
		y = (p1[1] + p2[1] + p3[1]) / 3
		center = ogr.Geometry(ogr.wkbPoint)
		center.AddPoint(x, y)
		if polygon.Contains(center):
			fs = (p2[0] - p1[0], p2[1] - p1[1])
			ft = (p3[0] - p1[0], p3[1] - p1[1])
			x = fs[0] * ft[1] - fs[1] * ft[0]
			if x < 0:
				return True
			elif x > 0:
				return False
	return False


def check_self_intersect(polygon):
	"""
	检查图形是否自相交
	:param polygon:
	:return:
	"""
	if polygon.IsValid():
		return False
	return True

def check_has_hole(polygon):
	"""
	检查图形是否有回字型
	:param polygon:
	:return:
	"""
	if polygon.GetGeometryType() != ogr.wkbPolygon or polygon.GetGeometryCount() != 1:
		return True
	return False

def check_polygon_isclose(polygon):
	"""
	检查多边形是否闭合
	:param polygon:
	:return:
	"""
	ring = polygon.GetGeometryRef(0)
	cnt = get_point_count(ring)
	# 检查是否闭合
	if ring.GetPoint(0) != ring.GetPoint(cnt - 1):
		return False
	return True