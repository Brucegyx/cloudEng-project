/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lambda;

import java.util.List;

/**
 *
 * @author wlloyd
 */
public class Response extends saaf.Response {
    
    //
    // User Defined Attributes
    //
    //
    // ADD getters and setters for custom attributes here.
    //

    private String status;
    public String getStatus(){
        return status;
    }
    public void setStatus(String status) {
        this.status = status;
    }
    private String message;
    public String getMessage(){
        return message;
    }
    public void setMessage(String message) {
        this.message = message;
    }
    // Return value
    private String value;
    public String getValue()
    {
        return value;
    }
    public void setValue(String value)
    {
        this.value = value;
    }
    
    public List<String> names;
    public List<String> getNames()
    {
        return this.names;
    }
    public void setNames(List<String> names)
    {
        this.names = names;
    }

    @Override
    public String toString()
    {
        return "value=" + this.getValue() + super.toString(); 
    }

}
