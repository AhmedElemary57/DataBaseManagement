package org.example;

import lombok.Data;
import lombok.NonNull;
import lombok.ToString;

import java.awt.*;

import static java.awt.Color.BLACK;
import static java.awt.Color.RED;

@Data
public class Node<T extends Comparable<T>> {
    @NonNull
    private T key;

    public T getVersion() {
        return version;
    }

    public void setVersion(T version) {
        this.version = version;
    }

    private T value;

    private T version;
    private int height;
    @ToString.Exclude
    private Node<T> parent, left, right;
    private Color color = RED;

    public Node(T key, T value, T version) {
        this.key = key;
        this.value = value;
        this.version = version;
        parent = left = right = null;
        height = 0;
    }
    public Node(T key, Node<T> left, Node<T> right, Node<T> parent){
        this.key = key;
        this.left = left;
        this.right = right;
        this.parent = parent;
        height = 0;
    }
    public void flipColor(){
        if (color==RED)
            color=BLACK;
        else
            color=RED;
    }
    public Node<T> getGrandParent(){
        return parent.getParent();
    }
    public Node<T> getUncle(){
        if (this.getGrandParent()!=null){
            Node<T> grandParent = this.getGrandParent();
            if (this.parent == grandParent.getLeft()){
                return this.getGrandParent().getRight();
            } else if (this.parent == grandParent.getRight()){
                return getGrandParent().getLeft();
            }

        }
        return null;
    }
    public boolean isLeafNode(){
        return (left==null && right == null);
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    public boolean isSiblingAndItsChildrenBlack(){
        return this.sibling().getColor()==BLACK &&
                ( this.sibling().getLeft()==null||this.sibling().getLeft().getColor()==BLACK) &&
                (this.sibling().getRight()==null||this.sibling().getRight().getColor()==BLACK );
    }
    public Node<T> sibling(){
        if (this.parent.getRight()==this)
            return parent.getLeft();
        else return parent.getRight();
    }
    public void clear() {
        if(parent != null){
            if (parent.getLeft()==this){
                parent.setLeft(null);
            }
            else if (parent.getRight()==this){
                parent.setRight(null);
            }
        }
        right=null;
        left=null;
        parent=null;
    }
    public boolean farChildIsBlackAndNearIsRedFromLeft(){
        return this.parent.getLeft()==this &&
                ( this.parent.right.right==null || this.parent.right.right.color==BLACK) &&
                this.parent.right.left.color==RED;

    }
    public boolean farChildIsBlackAndNearIsRedFromRight(){
        return this.parent.getRight()==this &&
                ( this.parent.left.left==null || this.parent.left.left.color==BLACK) &&
                this.parent.left.right.color==RED;

    }
    public boolean farChildIsRedFromLeft(){
        return this.parent.getLeft()==this &&
                this.parent.right.right.color==RED;
    }
    public boolean farChildIsRedFromRight(){
        return this.parent.getRight()==this &&
                this.parent.left.left.color==RED;
    }
}