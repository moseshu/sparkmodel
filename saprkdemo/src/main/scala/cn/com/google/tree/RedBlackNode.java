package cn.com.google.tree;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author Tobias
 */
public class RedBlackNode<E>
{
    private E data;
    private RedBlackNode<E> left;
    private RedBlackNode<E> right;
    private boolean isRed;

    public RedBlackNode(E data)
    {
        this.data = data;
        this.left = null;
        this.right = null;
        this.isRed = true;
    }

    public E getData()
    {
        return data;
    }

    public void setData(E data)
    {
        this.data = data;
    }

    public RedBlackNode<E> getLeft()
    {
        return left;
    }

    public void setLeft(RedBlackNode left)
    {
        this.left = left;
    }

    public RedBlackNode<E> getRight()
    {
        return right;
    }

    public void setRight(RedBlackNode right)
    {
        this.right = right;
    }

    public boolean isRed()
    {
        return isRed;
    }

    public void setIsRed(boolean isRed)
    {
        this.isRed = isRed;
    }




}